(ns otel.sdk
  "One-call SDK setup, and the global handles instrumentation reaches for.

  Most applications want the same thing: read the standard OTEL_* environment
  variables, batch spans to a collector, poll the runtime metrics, and shut down
  cleanly. `init!` does exactly that and returns a handle to close.

      (require '[otel.sdk :as sdk] '[otel.trace :as trace])

      (def otel (sdk/init! {:service-name \"checkout\"}))

      (trace/with-span [sp (sdk/tracer \"checkout.http\") \"GET /cart\"]
        (trace/set-attribute! sp :http.route \"/cart\"))

      (sdk/shutdown! otel)

  Nothing here is required to use the library — a caller who wants explicit
  control builds `otel.sdk.tracer/tracer-provider` directly and never touches
  this namespace. What this adds is the global registry, which exists so a
  library can call `(sdk/tracer \"my.lib\")` at any point and get a working tracer
  without the application having to thread a provider into it."
  (:refer-clojure :exclude [name])
  (:require [clojure.string :as str]
            [otel.bridge.tools-logging :as tools-logging]
            [otel.exporter.otlp :as otlp]
            [otel.exporter.stdout :as stdout]
            [otel.instrument.runtime :as runtime]
            [otel.logs :as logs-api]
            [otel.metrics :as metrics-api]
            [otel.propagation :as propagation]
            [otel.resource :as res]
            [otel.sdk.export :as export]
            [otel.sdk.logs :as sdk-logs]
            [otel.sdk.metrics :as sdk-metrics]
            [otel.sdk.sampler :as sampler]
            [otel.sdk.tracer :as sdk-tracer]
            [otel.trace :as trace]))

;; --- global registry --------------------------------------------------------

(defonce ^:private global (atom {:tracer-provider nil :meter-provider nil :logger-provider nil}))

(defn tracer-provider
  "The installed tracer provider, or nil."
  []
  (:tracer-provider @global))

(defn meter-provider
  "The installed meter provider, or nil."
  []
  (:meter-provider @global))

(defn logger-provider
  "The installed logger provider, or nil."
  []
  (:logger-provider @global))

(defn tracer
  "A tracer for `scope-name` from the installed provider. Falls back to the API's
  no-op tracer when no SDK has been installed, so instrumentation is safe to
  write before — or without — any configuration."
  ([scope-name] (tracer scope-name {}))
  ([scope-name opts]
   (if-let [p (tracer-provider)]
     (sdk-tracer/get-tracer p (assoc opts :name scope-name))
     trace/noop-tracer)))

(defn meter
  "A meter for `scope-name` from the installed provider, or the no-op meter."
  ([scope-name] (meter scope-name {}))
  ([scope-name opts]
   (if-let [p (meter-provider)]
     (sdk-metrics/get-meter p (assoc opts :name scope-name))
     metrics-api/noop-meter)))

(defn logger
  "A logger for `scope-name` from the installed provider, or the no-op logger.
  Most code should not need this — configure `:logs? true` and keep using
  clojure.tools.logging, which the bridge routes here."
  ([scope-name] (logger scope-name {}))
  ([scope-name opts]
   (if-let [p (logger-provider)]
     (sdk-logs/get-logger p (assoc opts :name scope-name))
     logs-api/noop-logger)))

;; --- configuration ----------------------------------------------------------

(defn- env [k] (jolt.host/getenv k))

(defn- env-sampler []
  (sampler/from-config (env "OTEL_TRACES_SAMPLER") (env "OTEL_TRACES_SAMPLER_ARG")))

(defn- env-exemplar-filter []
  (when-let [configured (some-> (env "OTEL_METRICS_EXEMPLAR_FILTER")
                                str/trim
                                str/lower-case)]
    (get {"always_on" :always-on
          "always_off" :always-off
          "trace_based" :trace-based}
         configured)))

(defn- disabled? []
  (= "true" (some-> (env "OTEL_SDK_DISABLED") str/trim str/lower-case)))

(def ^:private exporter-kinds #{:otlp :console :json :none})

(defn- check-exporter
  "`:exporter` is a kind or an exporter instance. Anything else is a mistake --
  a typo used to fall through to OTLP, so telemetry silently went to
  localhost:4318 instead of where it was asked to go."
  [exporter]
  (when-not (or (contains? exporter-kinds exporter)
                (satisfies? export/SpanExporter exporter)
                (satisfies? export/MetricExporter exporter)
                (satisfies? sdk-logs/LogRecordExporter exporter))
    (throw (ex-info (str "unknown :exporter " (pr-str exporter)
                         " -- expected one of " (str/join ", " (sort (map str exporter-kinds)))
                         ", or an exporter instance")
                    {:exporter exporter :kinds exporter-kinds})))
  exporter)

;; An instance is used for whichever signals it actually implements; the others
;; get no exporter rather than quietly falling back to the network.
(defn- build-span-exporter [exporter opts]
  (case exporter
    :none nil
    :console (stdout/exporter {})
    :json (stdout/json-exporter {})
    :otlp (otlp/exporter opts)
    (when (satisfies? export/SpanExporter exporter) exporter)))

(defn- build-metric-exporter [exporter opts]
  (case exporter
    :none nil
    (:console :json) (stdout/metric-exporter {})
    :otlp (otlp/metric-exporter opts)
    (when (satisfies? export/MetricExporter exporter) exporter)))

(defn- build-log-exporter [exporter opts]
  (case exporter
    :none nil
    (:console :json) (stdout/log-exporter {})
    :otlp (otlp/log-exporter opts)
    (when (satisfies? sdk-logs/LogRecordExporter exporter) exporter)))

(defn init!
  "Configure and install a tracing (and, unless disabled, metrics) SDK.

  Options — every one falls back to its standard OTEL_* environment variable, so
  a deployment can be configured without touching code:

    :service-name     sets service.name (OTEL_SERVICE_NAME)
    :resource         a resource merged over the detected defaults
    :sampler          a sampler (OTEL_TRACES_SAMPLER / _ARG)
    :exporter         :otlp (default), :console, :json, :none, or an exporter
                      instance -- which is used for whichever signals it
                      implements, so a memory exporter in a test collects spans
                      without metrics reaching the network
    :endpoint         OTLP base endpoint (OTEL_EXPORTER_OTLP_ENDPOINT)
    :headers          extra OTLP request headers
    :processor        :batch (default) or :simple
    :metrics?         collect metrics (default true)
    :runtime-metrics? register the Chez runtime instruments (default true)
    :metric-interval-ms  metric collection period (default 60000)
    :exemplar-filter   :trace-based (default), :always-on, or :always-off
                       (OTEL_METRICS_EXEMPLAR_FILTER)
    :logs?            emit the logs signal (default false)
    :bridge-logging?  route clojure.tools.logging through it (default true when
                      :logs? is on) -- additive, the existing backend keeps working

  Returns a handle for `shutdown!`. Honours OTEL_SDK_DISABLED=true by installing
  nothing, which is how the spec says to turn telemetry off without code changes."
  ([] (init! {}))
  ([{:keys [service-name resource sampler exporter endpoint headers processor
            metrics? runtime-metrics? metric-interval-ms logs? bridge-logging?]
     :or {exporter :otlp processor :batch metrics? true runtime-metrics? true
          logs? false bridge-logging? true}
     :as opts}]
   (check-exporter exporter)
   (if (disabled?)
     {:disabled? true}
     (let [base (res/merge-resources
                  (res/default-resource)
                  (cond-> (or resource res/empty-resource)
                    service-name (res/merge-resources (res/resource {:service.name service-name}))))
           span-exporter (build-span-exporter exporter (select-keys opts [:endpoint :headers :traces-url :timeout-ms]))
           processors (if span-exporter
                        [(if (= :simple processor)
                           (export/simple-processor span-exporter)
                           (export/batch-processor span-exporter (select-keys opts [:schedule-delay-ms
                                                                                    :max-queue-size
                                                                                    :max-export-batch-size])))]
                        [])
           tp (sdk-tracer/tracer-provider {:resource base
                                           :sampler (or sampler (env-sampler) sampler/default-sampler)
                                           :processors processors
                                           :limits (:limits opts)})
           mp (when metrics?
                (sdk-metrics/meter-provider {:resource base
                                             :temporality (:temporality opts)
                                             :exemplar-filter (or (:exemplar-filter opts)
                                                                  (env-exemplar-filter)
                                                                  :trace-based)
                                             :exemplar-reservoir-size (:exemplar-reservoir-size opts)}))
           metric-exporter (when mp (build-metric-exporter exporter
                                                           (select-keys opts [:endpoint :headers :metrics-url :timeout-ms])))
           reader (when metric-exporter
                    (sdk-metrics/periodic-reader mp metric-exporter
                                                 {:interval-ms (or metric-interval-ms 60000)}))
           log-exporter (when logs?
                          (build-log-exporter exporter
                                              (select-keys opts [:endpoint :headers :logs-url :timeout-ms])))
           lp (when log-exporter
                (sdk-logs/logger-provider
                  {:resource base
                   :processors [(if (= :simple processor)
                                  (sdk-logs/simple-processor log-exporter)
                                  (sdk-logs/batch-processor log-exporter {}))]}))
           ;; Off by default: installing it rewires the application's logging, which
           ;; is too big a side effect to take without being asked.
           previous-factory (when (and lp bridge-logging?) (tools-logging/install! lp))]
       (when (and mp runtime-metrics?)
         (runtime/register! (sdk-metrics/get-meter mp {:name "otel.instrument.runtime"
                                                       :version res/sdk-version})))
       (swap! global assoc :tracer-provider tp :meter-provider mp :logger-provider lp)
       {:tracer-provider tp
        :meter-provider mp
        :logger-provider lp
        :reader reader
        :previous-logger-factory previous-factory
        :propagator propagation/default-propagator}))))

(defn force-flush!
  "Push everything buffered to the exporters now."
  [handle]
  (boolean
    (and (some-> (:tracer-provider handle) sdk-tracer/force-flush!)
         (or (nil? (:reader handle)) (export/force-flush! (:reader handle)))
         (or (nil? (:logger-provider handle)) (sdk-logs/force-flush! (:logger-provider handle))))))

(defn shutdown!
  "Flush and stop everything `init!` started, and clear the global registry.

  Call this before the process exits: a batch processor holds spans that have not
  been sent yet, and they are lost if the process just ends."
  [handle]
  ;; The logging bridge is removed first: once the logger provider is shut down,
  ;; a bridged log call would emit into a dead provider on every line.
  (when-let [previous (:previous-logger-factory handle)]
    (tools-logging/uninstall! previous))
  (let [ok (cond-> true
             (:reader handle) (and (export/shutdown! (:reader handle)))
             (:logger-provider handle) (and (sdk-logs/shutdown! (:logger-provider handle)))
             (:meter-provider handle) (and (sdk-metrics/shutdown! (:meter-provider handle)))
             (:tracer-provider handle) (and (sdk-tracer/shutdown! (:tracer-provider handle))))]
    (reset! global {:tracer-provider nil :meter-provider nil :logger-provider nil})
    ok))
