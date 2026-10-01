(ns otel.sdk-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.tools.logging.impl :as impl]
            [otel.exporter.memory :as mem]
            [otel.logs :as logs]
            [otel.metrics :as metrics]
            [otel.resource :as res]
            [otel.sdk :as sdk]
            [otel.sdk.metrics :as sdk-metrics]
            [otel.trace :as trace]))

(defn- with-sdk [opts f]
  (let [handle (sdk/init! (merge {:exporter :none} opts))]
    (try (f handle)
         (finally (sdk/shutdown! handle)))))

(deftest init-installs-a-global-tracer
  (with-sdk {:service-name "checkout"}
    (fn [_]
      (is (some? (sdk/tracer-provider)))
      (let [t (sdk/tracer "my.lib")]
        (trace/with-span [sp t "op"]
          (is (trace/recording? sp))
          (is (trace/valid? (trace/span-context-of sp))))))))

(deftest service-name-reaches-the-resource
  (with-sdk {:service-name "checkout"}
    (fn [handle]
      (is (= "checkout"
             (get (res/attributes (:resource (:tracer-provider handle))) "service.name"))))))

(deftest the-detected-resource-is-still-present
  (with-sdk {:service-name "checkout"}
    (fn [handle]
      (let [a (res/attributes (:resource (:tracer-provider handle)))]
        (is (= "opentelemetry" (get a "telemetry.sdk.name")))
        (is (= "Chez Scheme" (get a "process.runtime.name")))))))

(deftest a-meter-is-installed-and-carries-runtime-instruments
  (with-sdk {}
    (fn [handle]
      (is (some? (sdk/meter-provider)))
      (let [names (->> (sdk-metrics/collect! (:meter-provider handle))
                       (mapcat :metrics)
                       (map :name)
                       set)]
        (is (contains? names "jolt.memory.used"))
        (is (contains? names "jolt.gc.count"))))))

(deftest runtime-metrics-can-be-turned-off
  (with-sdk {:runtime-metrics? false}
    (fn [handle]
      (is (empty? (mapcat :metrics (sdk-metrics/collect! (:meter-provider handle))))))))

(deftest metrics-can-be-turned-off-entirely
  (with-sdk {:metrics? false}
    (fn [handle]
      (is (nil? (:meter-provider handle)))
      (testing "asking for a meter still yields a working no-op"
        (is (some? (metrics/add! (metrics/counter (sdk/meter "x") "c") 1)))))))

(deftest without-an-sdk-the-api-is-a-noop
  (testing "instrumentation written against sdk/tracer must work before init!"
    (sdk/shutdown! {})
    (let [t (sdk/tracer "my.lib")]
      (trace/with-span [sp t "op"]
        (is (not (trace/recording? sp)))))))

(deftest shutdown-clears-the-global-registry
  (let [handle (sdk/init! {:exporter :none})]
    (is (some? (sdk/tracer-provider)))
    (sdk/shutdown! handle)
    (is (nil? (sdk/tracer-provider)))
    (is (nil? (sdk/meter-provider)))))

(deftest logs-are-off-by-default
  (with-sdk {}
    (fn [handle]
      (is (nil? (:logger-provider handle)))
      (testing "asking for a logger still yields a working no-op"
        (is (some? (logs/emit! (sdk/logger "x") {:body "b" :severity :info})))))))

(deftest logs-can-be-enabled
  (let [handle (sdk/init! {:exporter :none :logs? true :metrics? false})]
    (try
      (testing ":exporter :none builds no log exporter, so no provider is installed"
        (is (nil? (:logger-provider handle))))
      (finally (sdk/shutdown! handle)))))

(deftest enabling-logs-installs-the-logging-bridge
  (let [before log/*logger-factory*
        handle (sdk/init! {:exporter :console :processor :simple :logs? true :metrics? false})]
    (try
      (is (some? (:logger-provider handle)))
      (is (some? (sdk/logger-provider)))
      (testing "the bridge wraps the factory that was already installed"
        (is (not= before log/*logger-factory*))
        (is (str/includes? (impl/name log/*logger-factory*) "jolt/stderr")))
      (finally (sdk/shutdown! handle)))
    (testing "shutdown restores the original factory"
      (is (= before log/*logger-factory*)))))

(deftest the-bridge-can-be-declined
  (let [before log/*logger-factory*
        handle (sdk/init! {:exporter :console :logs? true :bridge-logging? false :metrics? false})]
    (try
      (is (= before log/*logger-factory*))
      (finally (sdk/shutdown! handle)))))

(deftest console-exporter-wiring
  (let [handle (sdk/init! {:exporter :console :processor :simple :metrics? false})]
    (try
      (trace/with-span [sp (sdk/tracer "s") "printed-span"])
      (is (sdk/force-flush! handle))
      (finally (sdk/shutdown! handle)))))

(deftest an-exporter-instance-can-be-handed-to-init
  (let [exp (mem/exporter)
        handle (sdk/init! {:exporter exp :processor :simple :metrics? false})]
    (try
      (trace/with-span [sp (sdk/tracer "s") "instance-span"])
      (is (sdk/force-flush! handle))
      (is (= ["instance-span"] (mapv :name (mem/spans exp))))
      (finally (sdk/shutdown! handle)))))

(deftest a-span-only-exporter-instance-does-not-become-a-metric-exporter
  (let [exp (mem/exporter)
        handle (sdk/init! {:exporter exp :processor :simple})]
    (try
      (is (some? (sdk/meter-provider)))
      (is (nil? (:reader handle)))
      (finally (sdk/shutdown! handle)))))

(deftest a-metric-exporter-instance-is-used-for-metrics
  (let [exp (mem/metric-exporter)
        handle (sdk/init! {:exporter exp :metric-interval-ms 0})]
    (try
      (let [c (metrics/counter (sdk/meter "m") "hits")]
        (metrics/add! c 1)
        (is (sdk/force-flush! handle))
        (is (pos? (count (mem/collections exp)))))
      (finally (sdk/shutdown! handle)))))

(deftest an-unknown-exporter-keyword-is-rejected
  (is (thrown? Exception (sdk/init! {:exporter :memroy})))
  (testing "the message names what was passed and what is valid"
    (try
      (sdk/init! {:exporter :memroy})
      (is false "expected a throw")
      (catch :default e
        (is (str/includes? (ex-message e) ":memroy"))
        (is (str/includes? (ex-message e) ":otlp"))))))
