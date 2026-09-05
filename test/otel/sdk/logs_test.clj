(ns otel.sdk.logs-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.logging :as log]
            [clojure.tools.logging.impl :as impl]
            [otel.bridge.tools-logging :as bridge]
            [otel.exporter.memory :as memory]
            [otel.logs :as logs]
            [otel.otlp.encode :as enc]
            [otel.resource :as res]
            [otel.sdk.clock :as clock]
            [otel.sdk.export :as export]
            [otel.sdk.logs :as sdk-logs]
            [otel.sdk.tracer :as sdk-tracer]
            [otel.trace :as trace]))

(defn- setup []
  (let [exporter (memory/log-exporter)
        provider (sdk-logs/logger-provider
                   {:resource (res/resource {:service.name "svc"})
                    :processors [(sdk-logs/simple-processor exporter)]
                    :clock (clock/fake-clock {:wall 1000 :mono 0})})]
    {:exporter exporter
     :provider provider
     :logger (sdk-logs/get-logger provider {:name "scope" :version "1.0"})}))

;; --- severity ---------------------------------------------------------------

(deftest severity-numbers-follow-the-spec-ranges
  (is (= 1 (logs/severity-number :trace)))
  (is (= 5 (logs/severity-number :debug)))
  (is (= 9 (logs/severity-number :info)))
  (is (= 13 (logs/severity-number :warn)))
  (is (= 17 (logs/severity-number :error)))
  (is (= 21 (logs/severity-number :fatal)))
  (is (= 0 (logs/severity-number :nonsense))))

;; --- emission ---------------------------------------------------------------

(deftest a-record-reaches-the-exporter
  (let [{:keys [logger exporter]} (setup)]
    (logs/emit! logger {:body "hello" :severity :info})
    (let [[r] (memory/records exporter)]
      (is (= "hello" (:body r)))
      (is (= 9 (:severity-number r)))
      (is (= "INFO" (:severity-text r)))
      (is (= 1000 (:observed-time-unix-nano r)))
      (testing "no explicit event time was given, so none is recorded"
        (is (nil? (:timestamp-unix-nano r)))))))

(deftest attributes-are-normalized
  (let [{:keys [logger exporter]} (setup)]
    (logs/emit! logger {:body "x" :severity :warn :attributes {:user.id 42 :dropped nil}})
    (let [[r] (memory/records exporter)]
      (is (= 42 (get (:attributes r) "user.id")))
      (is (not (contains? (:attributes r) "dropped"))))))

(deftest scope-and-resource-are-recorded
  (let [{:keys [logger exporter]} (setup)]
    (logs/emit! logger {:body "x" :severity :info})
    (let [[r] (memory/records exporter)]
      (is (= "scope" (get-in r [:scope :name])))
      (is (= "svc" (get (res/attributes (:resource r)) "service.name"))))))

(deftest an-explicit-event-timestamp-is-kept
  (let [{:keys [logger exporter]} (setup)]
    (logs/emit! logger {:body "x" :severity :info :timestamp 55})
    (is (= 55 (:timestamp-unix-nano (first (memory/records exporter)))))))

;; --- correlation ------------------------------------------------------------

(deftest a-record-emitted-in-a-span-carries-its-ids
  (testing "this correlation is the reason to route logs through OpenTelemetry at all"
    (let [{:keys [logger exporter]} (setup)
          span-exporter (memory/exporter)
          tp (sdk-tracer/tracer-provider {:resource res/empty-resource
                                          :processors [(export/simple-processor span-exporter)]})
          tracer (sdk-tracer/get-tracer tp {:name "t"})]
      (trace/with-span [sp tracer "op"]
        (logs/emit! logger {:body "inside" :severity :info}))
      (let [[r] (memory/records exporter)
            [s] (memory/spans span-exporter)]
        (is (= (get-in s [:span-context :trace-id]) (:trace-id r)))
        (is (= (get-in s [:span-context :span-id]) (:span-id r)))
        (is (= (bit-or trace/flag-sampled trace/flag-random)
               (:trace-flags r)))))))

(deftest a-record-outside-a-span-has-no-ids
  (let [{:keys [logger exporter]} (setup)]
    (logs/emit! logger {:body "outside" :severity :info})
    (let [[r] (memory/records exporter)]
      (is (nil? (:trace-id r)))
      (is (nil? (:span-id r))))))

;; --- shutdown ---------------------------------------------------------------

(deftest emission-stops-after-shutdown
  (let [{:keys [logger provider exporter]} (setup)]
    (logs/emit! logger {:body "before" :severity :info})
    (sdk-logs/shutdown! provider)
    (logs/emit! logger {:body "after" :severity :info})
    (is (= ["before"] (map :body (memory/records exporter))))))

;; --- batch processor --------------------------------------------------------

(deftest batch-processor-flushes-on-demand
  (let [exporter (memory/log-exporter)
        proc (sdk-logs/batch-processor exporter {:schedule-delay-ms 60000})
        provider (sdk-logs/logger-provider {:resource res/empty-resource :processors [proc]})
        logger (sdk-logs/get-logger provider {:name "s"})]
    (try
      (dotimes [i 3] (logs/emit! logger {:body (str "m" i) :severity :info}))
      (is (= [] (memory/records exporter)))
      (sdk-logs/force-flush! provider)
      (is (= 3 (count (memory/records exporter))))
      (finally (sdk-logs/shutdown! provider)))))

(deftest batch-processor-drops-when-full
  (let [exporter (memory/log-exporter)
        proc (sdk-logs/batch-processor exporter {:schedule-delay-ms 60000 :max-queue-size 2})
        provider (sdk-logs/logger-provider {:resource res/empty-resource :processors [proc]})
        logger (sdk-logs/get-logger provider {:name "s"})]
    (try
      (dotimes [i 10] (logs/emit! logger {:body (str "m" i) :severity :info}))
      (sdk-logs/force-flush! provider)
      (is (= 2 (count (memory/records exporter))))
      (is (= 8 (sdk-logs/dropped-count proc)))
      (finally (sdk-logs/shutdown! provider)))))

;; --- otlp encoding ----------------------------------------------------------

(deftest encodes-a-log-record
  (let [{:keys [logger exporter]} (setup)]
    (logs/emit! logger {:body "hello" :severity :error :attributes {:k "v"}})
    (let [req (enc/logs-request (memory/records exporter))
          r (-> req :resourceLogs first :scopeLogs first :logRecords first)]
      (is (= {:stringValue "hello"} (:body r)))
      (is (= 17 (:severityNumber r)))
      (is (= "ERROR" (:severityText r)))
      (is (string? (:observedTimeUnixNano r)))
      (is (= [{:key "k" :value {:stringValue "v"}}] (:attributes r)))
      (testing "no event time was set, so timeUnixNano is omitted rather than zero"
        (is (not (contains? r :timeUnixNano)))))))

(deftest encodes-correlation-ids
  (let [{:keys [logger exporter]} (setup)
        tp (sdk-tracer/tracer-provider {:resource res/empty-resource})
        tracer (sdk-tracer/get-tracer tp {:name "t"})]
    (trace/with-span [sp tracer "op"]
      (logs/emit! logger {:body "x" :severity :info}))
    (let [r (-> (enc/logs-request (memory/records exporter))
                :resourceLogs first :scopeLogs first :logRecords first)]
      (is (re-matches #"[0-9a-f]{32}" (:traceId r)))
      (is (re-matches #"[0-9a-f]{16}" (:spanId r))))))

;; --- tools.logging bridge ---------------------------------------------------

(defn- current-factory-name []
  (impl/name log/*logger-factory*))

(deftest the-bridge-emits-otel-records-for-log-calls
  (let [{:keys [provider exporter]} (setup)
        previous (bridge/install! provider)]
    (try
      (log/info "bridged message")
      (let [[r] (memory/records exporter)]
        (is (= "bridged message" (:body r)))
        (is (= 9 (:severity-number r)))
        (is (= "otel.sdk.logs-test" (get (:attributes r) "logger.name"))))
      (finally (bridge/uninstall! previous)))))

(deftest the-bridge-keeps-the-existing-backend
  (testing "installing telemetry must not silence the logs that were already working"
    (let [{:keys [provider]} (setup)
          previous log/*logger-factory*
          restored (bridge/install! provider)]
      (try
        (is (= previous restored) "install! returns the factory it wrapped")
        (is (clojure.string/includes? (current-factory-name) "jolt/stderr")
            "the original factory is still named inside the wrapper")
        (finally (bridge/uninstall! restored))))))

(deftest the-bridge-correlates-with-the-active-span
  (let [{:keys [provider exporter]} (setup)
        tp (sdk-tracer/tracer-provider {:resource res/empty-resource})
        tracer (sdk-tracer/get-tracer tp {:name "t"})
        previous (bridge/install! provider)]
    (try
      (trace/with-span [sp tracer "op"]
        (log/warn "inside a span"))
      (let [[r] (memory/records exporter)]
        (is (not (trace/valid? (trace/current-span-context)))
            "the span has ended by the time we assert")
        (is (some? (:trace-id r)) "but the record captured its ids at emit time")
        (is (re-matches #"[0-9a-f]{32}" (:trace-id r)))
        (is (= 13 (:severity-number r))))
      (finally (bridge/uninstall! previous)))))

(deftest the-bridge-records-a-throwable
  (let [{:keys [provider exporter]} (setup)
        previous (bridge/install! provider)]
    (try
      (log/error (ex-info "boom" {:a 1}) "it failed")
      (let [[r] (memory/records exporter)]
        (is (= "it failed" (:body r)))
        (is (= "boom" (get (:attributes r) "exception.message")))
        (is (string? (get (:attributes r) "exception.type"))))
      (finally (bridge/uninstall! previous)))))

(deftest a-failing-otel-path-does-not-break-logging
  (testing "the delegate runs first, so telemetry trouble never costs a log line"
    (let [broken (reify logs/LoggerProvider
                   (get-logger* [_ _]
                     (reify logs/Logger
                       (log-enabled? [_ _] true)
                       (emit! [_ _] (throw (ex-info "exporter down" {}))))))
          previous (bridge/install! broken)]
      (try
        (is (nil? (log/info "still logged")))
        (finally (bridge/uninstall! previous))))))
