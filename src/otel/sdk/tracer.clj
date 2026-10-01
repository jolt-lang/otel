(ns otel.sdk.tracer
  "The SDK tracer provider: the object an application configures once at startup
  and the tracers it hands out.

  The provider owns everything that is a whole-process decision — the resource,
  the sampler, the span limits, the clock, and the processor pipeline — and a
  tracer is a thin handle that stamps its instrumentation scope onto the spans it
  creates. That split is why a library can take a tracer at load time without
  knowing or caring how the application configured export."
  (:require [otel.context :as ctx]
            [otel.id :as id]
            [otel.resource :as res]
            [otel.sdk.clock :as clock]
            [otel.sdk.export :as export]
            [otel.sdk.sampler :as sampler]
            [otel.sdk.span :as span]
            [otel.trace :as trace]))

(defrecord SdkTracer [provider scope]
  trace/Tracer
  (start-span* [_ name opts]
    (let [{:keys [resource sampler limits clock processor shutdown?]} provider]
      (if @shutdown?
        ;; After shutdown nothing can be exported, so a span would only cost
        ;; memory. It still propagates: a non-recording span keeps the trace
        ;; intact for anything downstream that is still running.
        (trace/non-recording-span trace/invalid-span-context)
        (let [parent-ctx (or (:parent opts) (ctx/current))
              parent-sc (trace/span-context-of (trace/span-from-context parent-ctx))
              parent? (trace/valid? parent-sc)
              ;; A child stays in its parent's trace; a root starts a new one.
              trace-id (if parent? (:trace-id parent-sc) (id/trace-id))
              span-id (id/span-id)
              kind (:kind opts :internal)
              links (vec (:links opts))
              decision (sampler/should-sample
                         sampler
                         {:parent-context parent-ctx
                          :trace-id trace-id
                          :name name
                          :kind kind
                          :attributes (:attributes opts)
                          :links links})
              sc (trace/span-context
                   {:trace-id trace-id
                    :span-id span-id
                    ;; The sampler owns bit 0. Bit 1 describes the trace id and
                    ;; therefore stays unchanged while a trace is continued.
                    ;; New trace ids come from otel.id's random generator.
                    :trace-flags
                    (bit-or (if (sampler/sampled? decision)
                              trace/flag-sampled
                              0)
                            (if parent?
                              (bit-and (or (:trace-flags parent-sc) 0)
                                       trace/flag-random)
                              trace/flag-random))
                    ;; The parent's trace state travels on unless the sampler
                    ;; replaced it — it is how vendors carry their own routing
                    ;; data along a trace.
                    :trace-state (or (:trace-state decision)
                                     (when parent? (:trace-state parent-sc))
                                     [])})]
          (if-not (sampler/recording? decision)
            (trace/non-recording-span sc)
            (let [sp (span/new-span
                       {:span-context sc
                        :parent-span-id (when parent? (:span-id parent-sc))
                        :name name
                        :kind kind
                        :scope scope
                        :resource resource
                        :start-time-unix-nano (or (:start-timestamp opts)
                                                  (clock/wall-nanos clock))
                        :clock clock
                        :limits limits
                        :processor processor
                        ;; A sampler may contribute attributes of its own; they
                        ;; are merged under the caller's, which win.
                        :attributes (merge (:attributes decision) (:attributes opts))
                        :links links})]
              (export/on-start processor sp parent-ctx)
              sp)))))))

(defrecord SdkTracerProvider [resource sampler limits clock processor shutdown?])

(defn tracer-provider
  "Build a tracer provider.

  Options:
    :resource    what produced the telemetry (default `res/default-resource`)
    :sampler     sampling policy (default parent-based + always-on)
    :processors  a sequence of span processors (default none — spans are recorded
                 but go nowhere)
    :limits      per-span limits, see `otel.sdk.span/default-limits`
    :clock       the clock to time spans with (default the system clock)

  The clock is anchored: timestamps stay epoch-based, but every interval within
  the process comes from the monotonic clock, so a wall-clock step cannot produce
  a span that ends before it started."
  [{:keys [resource sampler processors limits clock]}]
  (->SdkTracerProvider (or resource (res/default-resource))
                       (or sampler sampler/default-sampler)
                       (span/span-limits limits)
                       (clock/anchored (or clock clock/system))
                       (export/composite-processor (or processors []))
                       (atom false)))

(defn get-tracer
  "A tracer for one instrumentation scope. `:name` identifies the instrumenting
  library (not the application) and is required; `:version` and `:schema-url` are
  optional but recommended, since a backend uses them to tell versions of an
  instrumentation apart."
  [provider {:keys [name version schema-url attributes]}]
  (->SdkTracer provider {:name name
                         :version version
                         :schema-url schema-url
                         :attributes (or attributes {})}))

(defn force-flush!
  "Block until everything already ended has been handed to the exporters."
  [provider]
  (export/force-flush! (:processor provider)))

(defn shutdown!
  "Flush and stop. The provider stops recording; call this before the process
  exits or buffered spans are lost."
  [provider]
  (reset! (:shutdown? provider) true)
  (export/shutdown! (:processor provider)))
