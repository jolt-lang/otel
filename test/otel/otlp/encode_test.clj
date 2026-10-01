(ns otel.otlp.encode-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [otel.exporter.memory :as memory]
            [otel.otlp.encode :as enc]
            [otel.otlp.json :as json]
            [otel.resource :as res]
            [otel.sdk.export :as export]
            [otel.sdk.tracer :as sdk]
            [otel.trace :as trace]))

;; --- json writer ------------------------------------------------------------

(deftest json-scalars
  (is (= "null" (json/write-str nil)))
  (is (= "true" (json/write-str true)))
  (is (= "false" (json/write-str false)))
  (is (= "42" (json/write-str 42)))
  (is (= "1.5" (json/write-str 1.5)))
  (is (= "\"x\"" (json/write-str "x"))))

(deftest json-escaping
  (is (= "\"a\\\"b\"" (json/write-str "a\"b")))
  (is (= "\"a\\\\b\"" (json/write-str "a\\b")))
  (is (= "\"a\\nb\"" (json/write-str "a\nb")))
  (testing "control characters have no shorthand and must use \\u"
    (is (= "\"\\u0001\"" (json/write-str (str (char 1)))))))

(deftest json-collections
  (is (= "[1,2,3]" (json/write-str [1 2 3])))
  (is (= "[]" (json/write-str [])))
  (is (= "{\"a\":1}" (json/write-str {:a 1})))
  (is (= "{}" (json/write-str {})))
  (is (= "{\"a\":{\"b\":[1,\"x\"]}}" (json/write-str {:a {:b [1 "x"]}}))))

(deftest json-preserves-large-integers-as-strings-when-asked
  (testing "the encoder, not the writer, is responsible for i64-as-string"
    (is (= "\"1785609674781645000\"" (json/write-str "1785609674781645000")))))

(deftest find-number-reads-a-field
  (is (= 3 (json/find-number "{\"rejectedSpans\":3}" "rejectedSpans")))
  (is (= 0 (json/find-number "{\"partialSuccess\":{\"rejectedSpans\":0}}" "rejectedSpans")))
  (is (nil? (json/find-number "{\"a\":1}" "rejectedSpans")))
  (is (nil? (json/find-number nil "rejectedSpans"))))

;; --- any values -------------------------------------------------------------

(deftest any-value-shapes
  (is (= {:stringValue "x"} (enc/any-value "x")))
  (is (= {:boolValue true} (enc/any-value true)))
  (is (= {:doubleValue 1.5} (enc/any-value 1.5)))
  (testing "an int is a decimal string — a JSON number is a double and would lose precision"
    (is (= {:intValue "42"} (enc/any-value 42))))
  (is (= {:arrayValue {:values [{:stringValue "a"} {:stringValue "b"}]}}
         (enc/any-value ["a" "b"]))))

(deftest key-values-shape
  (is (= [{:key "a" :value {:intValue "1"}}] (enc/key-values {"a" 1}))))

;; --- span encoding ----------------------------------------------------------

(defn- export-one
  "Run `f` against a tracer and return the single exported span, OTLP-encoded."
  [f]
  (let [exporter (memory/exporter)
        provider (sdk/tracer-provider {:resource (res/resource {:service.name "svc"})
                                       :processors [(export/simple-processor exporter)]})
        tracer (sdk/get-tracer provider {:name "scope" :version "9.9"})]
    (f tracer)
    (-> (enc/traces-request (memory/spans exporter))
        :resourceSpans first :scopeSpans first :spans first)))

(deftest encodes-the-core-span-fields
  (let [s (export-one (fn [t] (trace/with-span [sp t "op"])))]
    (is (= "op" (:name s)))
    (is (= 1 (:kind s)) "internal")
    (is (re-matches #"[0-9a-f]{32}" (:traceId s)))
    (is (re-matches #"[0-9a-f]{16}" (:spanId s)))
    (testing "timestamps are decimal strings, not numbers"
      (is (string? (:startTimeUnixNano s)))
      (is (string? (:endTimeUnixNano s)))
      (is (re-matches #"\d+" (:startTimeUnixNano s))))))

(deftest encodes-span-kind-as-an-integer
  (doseq [[kind code] {:internal 1 :server 2 :client 3 :producer 4 :consumer 5}]
    (let [s (export-one (fn [t] (trace/with-span [sp t "op" {:kind kind}])))]
      (is (= code (:kind s)) (str "kind " kind)))))

(deftest encodes-status
  (let [s (export-one (fn [t] (trace/with-span [sp t "op"] (trace/set-status! sp :error "bad"))))]
    (is (= 2 (get-in s [:status :code])))
    (is (= "bad" (get-in s [:status :message]))))
  (let [s (export-one (fn [t] (trace/with-span [sp t "op"] (trace/set-status! sp :ok))))]
    (is (= 1 (get-in s [:status :code])))
    (is (not (contains? (:status s) :message)))))

(deftest encodes-attributes-and-events
  (let [s (export-one (fn [t] (trace/with-span [sp t "op"]
                                (trace/set-attribute! sp :http.method "GET")
                                (trace/add-event! sp "cache.miss" {:key "k"}))))]
    (is (= [{:key "http.method" :value {:stringValue "GET"}}] (:attributes s)))
    (let [[e] (:events s)]
      (is (= "cache.miss" (:name e)))
      (is (string? (:timeUnixNano e)))
      (is (= [{:key "key" :value {:stringValue "k"}}] (:attributes e))))))

(deftest encodes-parent-and-flags
  (let [exporter (memory/exporter)
        provider (sdk/tracer-provider {:processors [(export/simple-processor exporter)]
                                       :resource res/empty-resource})
        tracer (sdk/get-tracer provider {:name "s"})]
    (trace/with-span [outer tracer "outer"]
      (trace/with-span [inner tracer "inner"]))
    (let [spans (:spans (first (:scopeSpans (first (:resourceSpans (enc/traces-request (memory/spans exporter)))))))
          inner (first (filter #(= "inner" (:name %)) spans))
          outer (first (filter #(= "outer" (:name %)) spans))]
      (is (= (:spanId outer) (:parentSpanId inner)))
      (is (not (contains? outer :parentSpanId)) "a root span has no parentSpanId")
      (is (= (bit-or trace/flag-sampled trace/flag-random) (:flags inner))
          "sampled and random-trace-id bits"))))

(deftest omits-empty-collections
  (testing "an empty attribute or event list is left out rather than sent as []"
    (let [s (export-one (fn [t] (trace/with-span [sp t "op"])))]
      (is (not (contains? s :attributes)))
      (is (not (contains? s :events)))
      (is (not (contains? s :links)))
      (is (not (contains? s :droppedAttributesCount))))))

(deftest groups-by-resource-and-scope
  (let [exporter (memory/exporter)
        provider (sdk/tracer-provider {:resource (res/resource {:service.name "svc"})
                                       :processors [(export/simple-processor exporter)]})
        t1 (sdk/get-tracer provider {:name "scope-a"})
        t2 (sdk/get-tracer provider {:name "scope-b"})]
    (trace/with-span [a t1 "a"])
    (trace/with-span [b t2 "b"])
    (trace/with-span [c t1 "c"])
    (let [req (enc/traces-request (memory/spans exporter))
          [rs] (:resourceSpans req)]
      (is (= 1 (count (:resourceSpans req))) "one resource block for one provider")
      (is (= 2 (count (:scopeSpans rs))) "one scope block per tracer")
      (is (= #{"scope-a" "scope-b"} (set (map #(get-in % [:scope :name]) (:scopeSpans rs)))))
      (is (= #{2 1} (set (map #(count (:spans %)) (:scopeSpans rs)))))
      (is (some #(= {:key "service.name" :value {:stringValue "svc"}} %)
                (get-in rs [:resource :attributes]))))))

(deftest the-whole-request-serializes
  (let [exporter (memory/exporter)
        provider (sdk/tracer-provider {:resource (res/default-resource)
                                       :processors [(export/simple-processor exporter)]})
        tracer (sdk/get-tracer provider {:name "s" :version "1"})]
    (trace/with-span [sp tracer "op"]
      (trace/set-attribute! sp :n 1)
      (trace/add-event! sp "e"))
    (let [s (json/write-str (enc/traces-request (memory/spans exporter)))]
      (is (str/starts-with? s "{\"resourceSpans\":["))
      (is (str/includes? s "\"scopeSpans\""))
      (is (str/includes? s "\"startTimeUnixNano\""))
      (testing "enums are integers, never the SDK's keyword names"
        (is (str/includes? s "\"kind\":1"))
        (is (not (str/includes? s "internal")))
        (is (not (str/includes? s "unset"))))
      (testing "timestamps are quoted, so no precision is lost to a JSON double"
        (is (re-find #"\"startTimeUnixNano\":\"\d{19}\"" s))))))
