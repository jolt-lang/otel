(ns otel.otlp.decode
  "Decoding OTLP/JSON back into the SDK's canonical values — the inverse of
  otel.otlp.encode, for a receiver or a test that needs to read what an exporter
  wrote.

  Decoding is strict about what it understands and lenient only where the specs
  require it:

    * Unknown fields are ignored, as the OTLP spec requires of JSON receivers so
      that newer senders keep working.
    * A JSON null, or an absent field, is the proto default.
    * 64-bit integers are accepted as decimal strings or as JSON integers, and
      doubles as numbers or strings, per the protobuf JSON mapping.
    * Everything else that cannot become a canonical value — a missing or doubled
      numeric arm, a malformed id or timestamp, a duplicate attribute key, a
      point type the SDK does not produce — throws ex-info naming the field."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [otel.id :as id]
            [otel.resource :as res]))

(defn- fail [msg data]
  (throw (ex-info msg data)))

;; --- primitives -------------------------------------------------------------

(def ^:private i64-min -9223372036854775808)
(def ^:private i64-max 9223372036854775807)

(defn- i64
  "A 64-bit integer from its decimal-string or JSON-integer form."
  [field v]
  (let [n (cond
            (and (string? v) (re-matches #"-?\d{1,19}" v))
            (try (Long/parseLong v) (catch :default _ nil))
            (integer? v) v)]
    (if (and (some? n) (<= i64-min n i64-max))
      n
      (fail (str "OTLP field " field " is not a 64-bit integer") {:field field :value v}))))

(defn- u64
  "A non-negative 64-bit integer. Timestamps and counts are unsigned on the wire;
  anything at or above 2^63 is centuries out and cannot be a real value here."
  [field v]
  (let [n (i64 field v)]
    (if (neg? n)
      (fail (str "OTLP field " field " must not be negative") {:field field :value v})
      n)))

(defn- f64 [field v]
  (cond
    (number? v) (double v)
    (= "NaN" v) Double/NaN
    (= "Infinity" v) Double/POSITIVE_INFINITY
    (= "-Infinity" v) Double/NEGATIVE_INFINITY
    (and (string? v) (re-matches #"-?\d+(\.\d+)?([eE][+-]?\d+)?" v)) (Double/parseDouble v)
    :else (fail (str "OTLP field " field " is not a double") {:field field :value v})))

(defn- object [field v]
  (cond
    (nil? v) {}
    (map? v) v
    :else (fail (str "OTLP field " field " must be an object") {:field field :value v})))

(defn- array [field v]
  (cond
    (nil? v) []
    (vector? v) v
    :else (fail (str "OTLP field " field " must be an array") {:field field :value v})))

(defn- text [field v]
  (cond
    (nil? v) nil
    (string? v) v
    :else (fail (str "OTLP field " field " must be a string") {:field field :value v})))

(defn- present
  "The keys of `m` among `arms` that carry a value."
  [m arms]
  (filterv #(some? (get m %)) arms))

;; --- attributes -------------------------------------------------------------

(def ^:private scalar-arms ["stringValue" "boolValue" "intValue" "doubleValue"])

(defn- scalar-value [v]
  (let [v (object "value" v)
        [arm & more] (present v scalar-arms)]
    (when (or (nil? arm) more)
      (fail "an attribute value needs exactly one scalar arm" {:value v}))
    (let [x (get v arm)]
      (case arm
        "stringValue" (or (text arm x) "")
        "boolValue" (if (boolean? x) x (fail "boolValue must be a boolean" {:value v}))
        "intValue" (i64 arm x)
        "doubleValue" (f64 arm x)))))

(defn any-value
  "A canonical attribute value from an OTLP AnyValue. Only the arms the SDK's
  attribute model can hold are accepted: scalars and arrays of scalars."
  [v]
  (let [v (object "value" v)
        arms (present v (conj scalar-arms "arrayValue" "kvlistValue" "bytesValue"))]
    (cond
      (not= 1 (count arms))
      (fail "an attribute value needs exactly one arm" {:value v})

      (= "arrayValue" (first arms))
      (mapv scalar-value (array "values" (get (object "arrayValue" (get v "arrayValue")) "values")))

      (#{"kvlistValue" "bytesValue"} (first arms))
      (fail (str "unsupported attribute value " (first arms)) {:value v})

      :else (scalar-value v))))

(defn key-values
  "A canonical attribute map from an OTLP KeyValue list. A repeated key is
  rejected rather than resolved, since either choice would silently drop data."
  [kvs]
  (reduce (fn [m kv]
            (let [kv (object "attributes[]" kv)
                  k (text "key" (get kv "key"))]
              (when (nil? k)
                (fail "an attribute needs a string key" {:attribute kv}))
              (when (contains? m k)
                (fail (str "duplicate attribute key " k) {:key k}))
              (when (nil? (get kv "value"))
                (fail (str "attribute " k " has no value") {:attribute kv}))
              (assoc m k (any-value (get kv "value")))))
          {}
          (array "attributes" kvs)))

;; --- metrics ----------------------------------------------------------------

(defn- number-value
  "A point's value from its asInt / asDouble one-of. Exactly one must be set:
  neither leaves the point without a value, and both make it ambiguous."
  [p]
  (let [arms (present p ["asInt" "asDouble"])]
    (case arms
      ["asInt"] (i64 "asInt" (get p "asInt"))
      ["asDouble"] (f64 "asDouble" (get p "asDouble"))
      (fail "a number value needs exactly one of asInt and asDouble" {:point p}))))

(defn- correlation-id
  "A trace or span id, or nil when absent. The empty string is the proto default
  for bytes, so it means absent too."
  [field valid? v]
  (let [s (text field v)]
    (when-not (str/blank? s)
      (let [s (str/lower-case s)]
        (if (valid? s)
          s
          (fail (str "OTLP field " field " is not a valid id") {:field field :value v}))))))

(defn exemplar
  "A canonical exemplar from an OTLP Exemplar."
  [e]
  (let [e (object "exemplars[]" e)
        ts (get e "timeUnixNano")
        _ (when (nil? ts) (fail "an exemplar requires timeUnixNano" {:exemplar e}))
        trace-id (correlation-id "traceId" id/valid-trace-id? (get e "traceId"))
        span-id (correlation-id "spanId" id/valid-span-id? (get e "spanId"))
        filtered (key-values (get e "filteredAttributes"))]
    (when (not= (some? trace-id) (some? span-id))
      (fail "an exemplar traceId and spanId must be present together" {:exemplar e}))
    (cond-> {:time-unix-nano (u64 "timeUnixNano" ts)
             :value (number-value e)}
      (seq filtered) (assoc :filtered-attributes filtered)
      trace-id (assoc :trace-id trace-id :span-id span-id))))

(defn- point-common [p]
  (let [ts (get p "timeUnixNano")
        start (get p "startTimeUnixNano")
        exemplars (mapv exemplar (array "exemplars" (get p "exemplars")))]
    (when (nil? ts) (fail "a data point requires timeUnixNano" {:point p}))
    (cond-> {:attributes (key-values (get p "attributes"))
             :time-unix-nano (u64 "timeUnixNano" ts)}
      (some? start) (assoc :start-time-unix-nano (u64 "startTimeUnixNano" start))
      (seq exemplars) (assoc :exemplars exemplars))))

(defn- number-point [p]
  (let [p (object "dataPoints[]" p)]
    (assoc (point-common p) :value (number-value p))))

(defn- histogram-point
  "A canonical histogram point and the bounds it was bucketed against. The bounds
  live on the metric in the canonical shape, so the caller checks they agree."
  [p]
  (let [p (object "dataPoints[]" p)
        bounds (mapv #(f64 "explicitBounds" %) (array "explicitBounds" (get p "explicitBounds")))
        buckets (mapv #(u64 "bucketCounts" %) (array "bucketCounts" (get p "bucketCounts")))
        n (u64 "count" (or (get p "count") 0))]
    (when (not= (count buckets) (inc (count bounds)))
      (fail "a histogram point needs one more bucket count than explicit bounds"
            {:bucket-counts buckets :explicit-bounds bounds}))
    (when (not= n (reduce + buckets))
      (fail "a histogram point's count must equal the sum of its bucket counts"
            {:count n :bucket-counts buckets}))
    [bounds
     (cond-> (assoc (point-common p) :count n :bucket-counts buckets)
       (some? (get p "sum")) (assoc :sum (f64 "sum" (get p "sum")))
       (some? (get p "min")) (assoc :min (f64 "min" (get p "min")))
       (some? (get p "max")) (assoc :max (f64 "max" (get p "max"))))]))

(defn- temporality [data]
  (case (get data "aggregationTemporality")
    1 :delta
    2 :cumulative
    (fail "aggregationTemporality must be 1 (delta) or 2 (cumulative)"
          {:aggregationTemporality (get data "aggregationTemporality")})))

(def ^:private point-types
  ["sum" "gauge" "histogram" "exponentialHistogram" "summary"])

(defn metric
  "A canonical metric from an OTLP Metric. Sum, gauge and histogram are what the
  SDK produces; the other point types are rejected rather than dropped."
  [m]
  (let [m (object "metrics[]" m)
        nm (text "name" (get m "name"))
        [kind & more] (present m point-types)
        data (object kind (get m kind))
        base (cond-> {:name nm}
               (not (str/blank? (get m "description"))) (assoc :description (text "description" (get m "description")))
               (not (str/blank? (get m "unit"))) (assoc :unit (text "unit" (get m "unit"))))]
    (when (str/blank? nm)
      (fail "a metric requires a name" {:metric m}))
    (when (or (nil? kind) more)
      (fail (str "metric " nm " needs exactly one point type") {:metric nm}))
    (case kind
      "sum" (assoc base :type :sum
                   :temporality (temporality data)
                   :monotonic? (true? (get data "isMonotonic"))
                   :data-points (mapv number-point (array "dataPoints" (get data "dataPoints"))))
      "gauge" (assoc base :type :gauge
                     :data-points (mapv number-point (array "dataPoints" (get data "dataPoints"))))
      "histogram" (let [decoded (mapv histogram-point (array "dataPoints" (get data "dataPoints")))
                        bounds (distinct (map first decoded))]
                    (when (next bounds)
                      (fail (str "histogram " nm " has points with different explicit bounds")
                            {:explicit-bounds bounds}))
                    (assoc base :type :histogram
                           :temporality (temporality data)
                           :explicit-bounds (vec (first bounds))
                           :data-points (mapv second decoded)))
      (fail (str "unsupported metric point type " kind) {:metric nm}))))

(defn- scope [scope-metrics]
  (let [s (object "scope" (get scope-metrics "scope"))
        attrs (key-values (get s "attributes"))]
    (cond-> {:name (or (text "name" (get s "name")) "")}
      (not (str/blank? (get s "version"))) (assoc :version (text "version" (get s "version")))
      (seq attrs) (assoc :attributes attrs)
      (not (str/blank? (get scope-metrics "schemaUrl")))
      (assoc :schema-url (text "schemaUrl" (get scope-metrics "schemaUrl"))))))

(defn- resource [resource-metrics]
  (let [r (object "resource" (get resource-metrics "resource"))
        schema-url (text "schemaUrl" (get resource-metrics "schemaUrl"))]
    (res/resource (key-values (get r "attributes"))
                  {:schema-url (when-not (str/blank? schema-url) schema-url)})))

(defn metrics-request
  "Decode an OTLP/JSON ExportMetricsServiceRequest.

  Returns one {:resource … :scope-metrics […]} per resourceMetrics entry, where
  :scope-metrics has the {:scope … :metrics […]} shape sdk.metrics/collect!
  produces — so `(encode/metrics-request resource scope-metrics)` re-encodes it."
  [s]
  (let [req (try (json/read-str s)
                 (catch :default e
                   (fail "malformed OTLP/JSON" {:cause (ex-message e)})))]
    (when-not (map? req)
      (fail "an OTLP/JSON request must be an object" {}))
    (mapv (fn [rm]
            (let [rm (object "resourceMetrics[]" rm)]
              {:resource (resource rm)
               :scope-metrics (mapv (fn [sm]
                                      (let [sm (object "scopeMetrics[]" sm)]
                                        {:scope (scope sm)
                                         :metrics (mapv metric (array "metrics" (get sm "metrics")))}))
                                    (array "scopeMetrics" (get rm "scopeMetrics")))}))
          (array "resourceMetrics" (get req "resourceMetrics")))))
