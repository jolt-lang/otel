(ns otel.sdk.logs
  "The logs SDK: log record processors and the provider that owns them.

  Structurally this mirrors the span pipeline — a provider holding a resource, a
  clock and a processor chain; simple and batch processors over an exporter — and
  deliberately so, since the two signals have the same delivery problem and the
  same answer to it.

  The one thing unique to logs is correlation, and it happens at emit time: a
  record picks up the active span's trace and span ids as it is created. Doing it
  later would be too late, because by the time a batch is exported the span that
  gave the record its meaning is long out of scope."
  (:require [otel.attributes :as attr]
            [otel.logs :as api]
            [otel.resource :as res]
            [otel.sdk.clock :as clock]
            [otel.sdk.export :as export]
            [otel.trace :as trace]))

(def default-limits
  {:attribute-count-limit 128
   :attribute-value-length-limit nil})

(defprotocol LogRecordExporter
  (export-logs! [exporter records]
    "Write a batch of log records. Returns truthy on success; must not throw.")
  (shutdown-log-exporter! [exporter]
    "Release the exporter's resources."))

;; --- processors -------------------------------------------------------------

(defn- export-quietly! [exporter records]
  (try (export-logs! exporter records) (catch :default _ false)))

(defrecord SimpleLogProcessor [exporter]
  export/SpanProcessor
  (on-start [_ _ _] nil)
  (on-end [_ record] (export-quietly! exporter [record]) nil)
  (force-flush! [_] true)
  (shutdown! [_] (boolean (shutdown-log-exporter! exporter))))

(defn simple-processor
  "Export each record as it is emitted, synchronously."
  [exporter]
  (->SimpleLogProcessor exporter))

(def default-batch-config
  {:max-queue-size 2048
   :max-export-batch-size 512
   :schedule-delay-ms 1000})

(defn- take-batch! [state n]
  (let [[old new] (swap-vals! state
                              (fn [s]
                                (let [q (:queue s)]
                                  (assoc s :queue (subvec q (min n (count q)))))))]
    (subvec (:queue old) 0 (- (count (:queue old)) (count (:queue new))))))

(defn- drain! [exporter state batch-size]
  (loop [ok true]
    (let [batch (take-batch! state batch-size)]
      (if (empty? batch)
        ok
        (recur (and (export-quietly! exporter batch) ok))))))

(defrecord BatchLogProcessor [exporter state config worker]
  export/SpanProcessor
  (on-start [_ _ _] nil)
  (on-end [_ record]
    ;; Bounded and dropping, for the same reason the span queue is: a slow
    ;; collector must not become application latency or unbounded memory.
    (swap! state
           (fn [s]
             (if (or (:shutdown? s) (>= (count (:queue s)) (:max-queue-size config)))
               (update s :dropped inc)
               (update s :queue conj record))))
    nil)
  (force-flush! [_] (boolean (drain! exporter state (:max-export-batch-size config))))
  (shutdown! [this]
    (swap! state assoc :shutdown? true)
    (export/force-flush! this)
    (when worker (try (.join worker 5000) (catch :default _ nil)))
    (boolean (shutdown-log-exporter! exporter))))

(defn dropped-count [processor] (:dropped @(:state processor)))

(defn batch-processor
  "Queue emitted records and export them from a background thread."
  ([exporter] (batch-processor exporter {}))
  ([exporter opts]
   (let [config (merge default-batch-config opts)
         state (atom {:queue [] :dropped 0 :shutdown? false})
         worker (Thread.
                  (fn []
                    (loop []
                      (loop [remaining (:schedule-delay-ms config)]
                        (when (and (pos? remaining) (not (:shutdown? @state)))
                          (let [slice (min 50 remaining)]
                            (Thread/sleep slice)
                            (recur (- remaining slice)))))
                      (drain! exporter state (:max-export-batch-size config))
                      (when-not (:shutdown? @state) (recur)))))]
     (.setDaemon worker true)
     (.start worker)
     (->BatchLogProcessor exporter state config worker))))

;; --- logger and provider ----------------------------------------------------

(defrecord SdkLogger [provider scope]
  api/Logger
  (log-enabled? [_ _] (not @(:shutdown? provider)))
  (emit! [this record]
    (when-not @(:shutdown? provider)
      (let [{:keys [clock resource limits processor]} provider
            now (clock/wall-nanos clock)
            level (:severity record)
            ;; The active span at emit time is what ties this record to a
            ;; request. Captured here because it is gone by export time.
            sc (trace/current-span-context)
            lim (attr/limits {:count-limit (:attribute-count-limit limits)
                              :value-length-limit (:attribute-value-length-limit limits)})]
        (export/on-end
          processor
          (cond-> {:body (:body record)
                   :severity-number (or (:severity-number record) (api/severity-number level))
                   :severity-text (or (:severity-text record) (api/severity-text level))
                   :timestamp-unix-nano (:timestamp record)
                   :observed-time-unix-nano (or (:observed-timestamp record) now)
                   :attributes (attr/normalize (:attributes record) lim)
                   :resource resource
                   :scope scope}
            (and (string? (:event-name record))
                 (not (empty? (:event-name record))))
            (assoc :event-name (:event-name record))
            (trace/valid? sc) (assoc :trace-id (:trace-id sc)
                                     :span-id (:span-id sc)
                                     :trace-flags (:trace-flags sc))))))
    this))

(defrecord SdkLoggerProvider [resource clock limits processor shutdown?]
  api/LoggerProvider
  (get-logger* [this scope] (->SdkLogger this scope)))

(defn logger-provider
  "Build a logger provider.

  Options: :resource, :clock, :processors (a sequence of log record processors),
  and :limits."
  [{:keys [resource clock processors limits]}]
  (->SdkLoggerProvider (or resource (res/default-resource))
                       (clock/anchored (or clock clock/system))
                       (merge default-limits limits)
                       (export/composite-processor (or processors []))
                       (atom false)))

(defn get-logger
  "A logger for one instrumentation scope."
  [provider {:keys [name version schema-url]}]
  (api/get-logger* provider {:name name :version version :schema-url schema-url}))

(defn force-flush! [provider] (export/force-flush! (:processor provider)))

(defn shutdown! [provider]
  (reset! (:shutdown? provider) true)
  (export/shutdown! (:processor provider)))
