(ns otel.otlp.decode-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.metrics :as api]
            [otel.otlp.decode :as dec]
            [otel.otlp.encode :as enc]
            [otel.otlp.json :as json]
            [otel.resource :as res]
            [otel.sdk.clock :as clock]
            [otel.sdk.metrics :as sdk]
            [otel.trace :as trace]))

(def trace-id "11111111111111111111111111111111")
(def span-id "2222222222222222")

(def wire-exemplar
  {"timeUnixNano" "123"
   "asInt" "7"
   "filteredAttributes" [{"key" "request.id" "value" {"stringValue" "r-1"}}]
   "traceId" trace-id
   "spanId" span-id})

;; --- exemplars --------------------------------------------------------------

(deftest decodes-a-correlated-exemplar
  (is (= {:time-unix-nano 123
          :value 7
          :filtered-attributes {"request.id" "r-1"}
          :trace-id trace-id
          :span-id span-id}
         (dec/exemplar wire-exemplar))))

(deftest decodes-numeric-arms
  (testing "asInt keeps an integer, from either a decimal string or a JSON number"
    (is (= 7 (:value (dec/exemplar wire-exemplar))))
    (is (= 7 (:value (dec/exemplar (assoc wire-exemplar "asInt" 7))))))
  (testing "asDouble keeps a double, even when written without a fraction"
    (let [e (dec/exemplar (-> wire-exemplar (dissoc "asInt") (assoc "asDouble" 1.5)))]
      (is (= 1.5 (:value e))))
    (let [e (dec/exemplar (-> wire-exemplar (dissoc "asInt") (assoc "asDouble" 50)))]
      (is (float? (:value e)))
      (is (= 50.0 (:value e))))))

(deftest an-uncorrelated-exemplar-has-no-ids
  (let [e (dec/exemplar (dissoc wire-exemplar "traceId" "spanId"))]
    (is (not (contains? e :trace-id)))
    (is (not (contains? e :span-id))))
  (testing "empty id strings are the proto default, so they mean absent"
    (let [e (dec/exemplar (assoc wire-exemplar "traceId" "" "spanId" ""))]
      (is (not (contains? e :trace-id))))))

(deftest omits-empty-filtered-attributes
  (is (not (contains? (dec/exemplar (dissoc wire-exemplar "filteredAttributes"))
                      :filtered-attributes)))
  (is (not (contains? (dec/exemplar (assoc wire-exemplar "filteredAttributes" []))
                      :filtered-attributes))))

(deftest rejects-malformed-exemplars
  (let [bad (fn [e] (is (thrown? Exception (dec/exemplar e)) (pr-str e)))]
    (testing "numeric arms: exactly one is required"
      (bad (dissoc wire-exemplar "asInt"))
      (bad (assoc wire-exemplar "asDouble" 1.5))
      (bad (assoc wire-exemplar "asInt" "7.5"))
      (bad (assoc wire-exemplar "asInt" "seven"))
      (bad (assoc wire-exemplar "asInt" 7.5))
      (bad (-> wire-exemplar (dissoc "asInt") (assoc "asDouble" "one"))))
    (testing "timestamps"
      (bad (dissoc wire-exemplar "timeUnixNano"))
      (bad (assoc wire-exemplar "timeUnixNano" "-1"))
      (bad (assoc wire-exemplar "timeUnixNano" "12x"))
      (bad (assoc wire-exemplar "timeUnixNano" 1.5))
      (bad (assoc wire-exemplar "timeUnixNano" "99999999999999999999")))
    (testing "ids"
      (bad (assoc wire-exemplar "traceId" "bad"))
      (bad (assoc wire-exemplar "spanId" "zzzzzzzzzzzzzzzz"))
      (bad (assoc wire-exemplar "traceId" (apply str (repeat 32 "0"))))
      (bad (assoc wire-exemplar "spanId" (apply str (repeat 16 "0"))))
      (bad (dissoc wire-exemplar "spanId"))
      (bad (assoc wire-exemplar "traceId" "")))
    (testing "filtered attributes"
      (bad (assoc wire-exemplar "filteredAttributes"
                  [{"key" "a" "value" {"stringValue" "x"}}
                   {"key" "a" "value" {"stringValue" "y"}}]))
      (bad (assoc wire-exemplar "filteredAttributes" [{"key" "a"}]))
      (bad (assoc wire-exemplar "filteredAttributes"
                  [{"key" "a" "value" {"stringValue" "x" "intValue" "1"}}]))
      (bad (assoc wire-exemplar "filteredAttributes" {"a" "x"})))))

(deftest decodes-attribute-values
  (is (= {"s" "x" "b" true "i" 3 "d" 2.5 "arr" ["a" "b"]}
         (dec/key-values [{"key" "s" "value" {"stringValue" "x"}}
                          {"key" "b" "value" {"boolValue" true}}
                          {"key" "i" "value" {"intValue" "3"}}
                          {"key" "d" "value" {"doubleValue" 2.5}}
                          {"key" "arr" "value" {"arrayValue" {"values" [{"stringValue" "a"}
                                                                        {"stringValue" "b"}]}}}]))))

;; --- whole requests ---------------------------------------------------------

(def sampled-context
  (trace/span-context {:trace-id trace-id :span-id span-id :sampled? true}))

(defn- collected-with-exemplars []
  (let [c (clock/fake-clock {:wall 1000 :mono 0})
        provider (sdk/meter-provider {:resource res/empty-resource
                                      :clock c
                                      :exemplar-filter :trace-based})
        meter (sdk/get-meter provider {:name "test" :version "1.0"})
        counter (api/counter meter "requests" {:unit "1"})
        gauge (api/gauge meter "temperature" {:description "air"})
        histogram (api/histogram meter "latency" {:boundaries [10.0 100.0]})]
    (trace/with-current-span (trace/non-recording-span sampled-context)
      (api/add! counter 3 {"route" "/a"})
      (api/set-value! gauge 21.5 {})
      (api/record! histogram 50.0 {}))
    (sdk/collect! provider)))

(defn- wire [collected]
  (json/write-str (enc/metrics-request (res/resource {"service.name" "svc"}) collected)))

(deftest sum-gauge-and-histogram-exemplars-round-trip
  (let [collected (collected-with-exemplars)
        s (wire collected)
        [{:keys [resource scope-metrics]} :as decoded] (dec/metrics-request s)]
    (is (= 1 (count decoded)))
    (is (= {"service.name" "svc"} (res/attributes resource)))
    (testing "every point type carries its exemplar through the wire"
      (let [by-name (into {} (map (juxt :name identity)) (:metrics (first scope-metrics)))
            exemplars (fn [nm] (-> (get by-name nm) :data-points first :exemplars))]
        (is (= [{:value 3 :time-unix-nano 1000
                 :trace-id trace-id :span-id span-id}]
               (mapv #(select-keys % [:value :time-unix-nano
                                      :trace-id :span-id])
                     (exemplars "requests"))))
        (is (= [21.5] (mapv :value (exemplars "temperature"))))
        (is (= [50.0] (mapv :value (exemplars "latency"))))
        (is (= trace-id (:trace-id (first (exemplars "latency")))))))
    (testing "re-encoding the decoded request reproduces the original wire form"
      (is (= s (wire scope-metrics))))))

(deftest decodes-metric-shapes
  (let [[{:keys [scope-metrics]}] (dec/metrics-request (wire (collected-with-exemplars)))
        {:keys [scope metrics]} (first scope-metrics)
        by-name (into {} (map (juxt :name identity)) metrics)]
    (is (= {:name "test" :version "1.0"} scope))
    (is (= {:type :sum :temporality :cumulative :monotonic? true :unit "1"}
           (select-keys (get by-name "requests") [:type :temporality :monotonic? :unit])))
    (is (= {:type :gauge :description "air"}
           (select-keys (get by-name "temperature") [:type :description])))
    (let [h (get by-name "latency")]
      (is (= [10.0 100.0] (:explicit-bounds h)))
      (is (= [0 1 0] (-> h :data-points first :bucket-counts)))
      (is (= 1 (-> h :data-points first :count))))))

(defn- request-with-point [metric]
  (json/write-str {"resourceMetrics"
                   [{"resource" {"attributes" []}
                     "scopeMetrics" [{"scope" {"name" "s"} "metrics" [metric]}]}]}))

(deftest rejects-malformed-requests
  (let [bad (fn [s] (is (thrown? Exception (dec/metrics-request s)) s))
        sum-point {"timeUnixNano" "2" "startTimeUnixNano" "1" "asInt" "1"}
        sum (fn [p] {"name" "m" "sum" {"dataPoints" [p] "aggregationTemporality" 2
                                       "isMonotonic" true}})]
    (bad "not json")
    (bad "[]")
    (bad (request-with-point (sum (assoc sum-point "exemplars" [(dissoc wire-exemplar "asInt")]))))
    (bad (request-with-point (sum (dissoc sum-point "asInt"))))
    (bad (request-with-point {"name" "m" "sum" {"dataPoints" [sum-point]
                                                "aggregationTemporality" 0}}))
    (bad (request-with-point {"name" "m"}))
    (bad (request-with-point {"name" "m" "summary" {"dataPoints" []}}))
    (testing "histogram bucket counts must match the bounds and the count"
      (let [h (fn [p] {"name" "h" "histogram" {"aggregationTemporality" 1 "dataPoints" [p]}})
            p {"timeUnixNano" "2" "startTimeUnixNano" "1" "count" "1" "sum" 5.0
               "bucketCounts" ["0" "1"] "explicitBounds" [10.0]}]
        (is (= 1 (count (dec/metrics-request (request-with-point (h p))))))
        (bad (request-with-point (h (assoc p "bucketCounts" ["1"]))))
        (bad (request-with-point (h (assoc p "count" "2"))))))))

(deftest ignores-unknown-fields
  (testing "OTLP/JSON receivers must ignore fields they do not know"
    (let [s (request-with-point {"name" "m" "futureField" 1
                                 "gauge" {"dataPoints" [{"timeUnixNano" "2" "asDouble" 1.0
                                                         "exemplars" [(assoc wire-exemplar "x" 1)]}]}})
          [{:keys [scope-metrics]}] (dec/metrics-request s)]
      (is (= 7 (-> scope-metrics first :metrics first :data-points first :exemplars first :value))))))
