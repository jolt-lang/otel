(ns otel.otlp.encode
  "Encoding SDK values into the OTLP wire shape (the JSON Protobuf encoding).

  Three rules from the OTLP spec drive everything here and are easy to get
  subtly wrong:

    * 64-bit integers are written as decimal STRINGS. JSON numbers are IEEE
      doubles, which cannot hold a nanosecond timestamp without losing precision,
      so a numeric startTimeUnixNano silently corrupts every span.
    * traceId and spanId are hex strings, NOT the base64 the standard protobuf
      JSON mapping would use for a bytes field.
    * enums are integers, and field names are lowerCamelCase.

  Spans are grouped resource -> scope -> spans, which is also the compression the
  format is designed around: the resource is written once per batch rather than
  once per span."
  (:require [otel.resource :as res]))

;; --- primitives -------------------------------------------------------------

(defn- i64
  "A 64-bit integer in its OTLP JSON form: a decimal string."
  [n]
  (str (or n 0)))

(defn any-value
  "An OTLP AnyValue for an already-normalized attribute value."
  [v]
  (cond
    (string? v) {:stringValue v}
    (or (true? v) (false? v)) {:boolValue v}
    (integer? v) {:intValue (i64 v)}
    (float? v) {:doubleValue (double v)}
    (sequential? v) {:arrayValue {:values (mapv any-value v)}}
    ;; normalize has already rejected anything else; a string keeps a surprise
    ;; value from failing the whole export.
    :else {:stringValue (str v)}))

(defn key-values
  "An OTLP KeyValue list for an attribute map."
  [attrs]
  (mapv (fn [[k v]] {:key (str k) :value (any-value v)}) attrs))

;; --- enums ------------------------------------------------------------------

(def span-kind-codes
  "SpanKind enum values from the OTLP proto."
  {:internal 1 :server 2 :client 3 :producer 4 :consumer 5})

(def status-codes
  "StatusCode enum values from the OTLP proto."
  {:unset 0 :ok 1 :error 2})

;; --- spans ------------------------------------------------------------------

(defn- trace-state-string [entries]
  (when (seq entries)
    (clojure.string/join "," (map (fn [[k v]] (str k "=" v)) entries))))

(defn- event->otlp [e]
  (cond-> {:timeUnixNano (i64 (:timestamp-unix-nano e))
           :name (:name e)}
    (seq (:attributes e)) (assoc :attributes (key-values (:attributes e)))))

(defn- link->otlp [l]
  (let [sc (:span-context l)]
    (cond-> {:traceId (:trace-id sc)
             :spanId (:span-id sc)}
      (seq (:trace-state sc)) (assoc :traceState (trace-state-string (:trace-state sc)))
      (seq (:attributes l)) (assoc :attributes (key-values (:attributes l))))))

(defn- status->otlp [status]
  (let [code (get status-codes (:code status) 0)]
    (cond-> {:code code}
      ;; A message is only carried for :error; the proto allows it elsewhere but
      ;; the spec says it is meaningless there.
      (and (= 2 code) (:description status)) (assoc :message (:description status)))))

(defn span->otlp
  "One SDK span map as an OTLP Span."
  [s]
  (let [sc (:span-context s)]
    (cond-> {:traceId (:trace-id sc)
             :spanId (:span-id sc)
             :name (:name s)
             :kind (get span-kind-codes (:kind s) 1)
             :startTimeUnixNano (i64 (:start-time-unix-nano s))
             :endTimeUnixNano (i64 (:end-time-unix-nano s))
             :status (status->otlp (:status s))
             ;; W3C trace flags, in the proto's flags field.
             :flags (or (:trace-flags sc) 0)}
      (:parent-span-id s) (assoc :parentSpanId (:parent-span-id s))
      (seq (:trace-state sc)) (assoc :traceState (trace-state-string (:trace-state sc)))
      (seq (:attributes s)) (assoc :attributes (key-values (:attributes s)))
      (pos? (or (:dropped-attributes-count s) 0)) (assoc :droppedAttributesCount (:dropped-attributes-count s))
      (seq (:events s)) (assoc :events (mapv event->otlp (:events s)))
      (pos? (or (:dropped-events-count s) 0)) (assoc :droppedEventsCount (:dropped-events-count s))
      (seq (:links s)) (assoc :links (mapv link->otlp (:links s)))
      (pos? (or (:dropped-links-count s) 0)) (assoc :droppedLinksCount (:dropped-links-count s)))))

(defn- scope->otlp [scope]
  (cond-> {:name (or (:name scope) "")}
    (:version scope) (assoc :version (:version scope))
    (seq (:attributes scope)) (assoc :attributes (key-values (:attributes scope)))))

(defn- resource->otlp [resource]
  {:attributes (key-values (res/attributes resource))})

(defn traces-request
  "A batch of SDK spans as an OTLP ExportTraceServiceRequest.

  Spans are grouped by resource and then by instrumentation scope — the nesting
  the format exists for, since it lets one resource block cover a whole batch."
  [spans]
  {:resourceSpans
   (mapv (fn [[resource by-resource]]
           (cond-> {:resource (resource->otlp resource)
                    :scopeSpans
                    (mapv (fn [[scope by-scope]]
                            (cond-> {:scope (scope->otlp scope)
                                     :spans (mapv span->otlp by-scope)}
                              (:schema-url scope) (assoc :schemaUrl (:schema-url scope))))
                          (group-by :scope by-resource))}
             (res/schema-url resource) (assoc :schemaUrl (res/schema-url resource))))
         (group-by :resource spans))})

;; --- metrics ----------------------------------------------------------------

(def temporality-codes
  "AggregationTemporality enum values from the OTLP proto."
  {:unspecified 0 :delta 1 :cumulative 2})

(defn- number-value
  "A NumberDataPoint's value field. Integers go in asInt as a decimal string, for
  the same precision reason timestamps do; floats go in asDouble."
  [v]
  (if (integer? v) {:asInt (i64 v)} {:asDouble (double v)}))

(defn- number-point [p]
  (cond-> (merge {:timeUnixNano (i64 (:time-unix-nano p))}
                 (number-value (:value p)))
    (:start-time-unix-nano p) (assoc :startTimeUnixNano (i64 (:start-time-unix-nano p)))
    (seq (:attributes p)) (assoc :attributes (key-values (:attributes p)))))

(defn- histogram-point [bounds p]
  (cond-> {:startTimeUnixNano (i64 (:start-time-unix-nano p))
           :timeUnixNano (i64 (:time-unix-nano p))
           :count (i64 (:count p))
           :sum (double (:sum p))
           :bucketCounts (mapv i64 (:bucket-counts p))
           :explicitBounds (mapv double bounds)}
    (seq (:attributes p)) (assoc :attributes (key-values (:attributes p)))
    (some? (:min p)) (assoc :min (double (:min p)))
    (some? (:max p)) (assoc :max (double (:max p)))))

(defn metric->otlp
  "One collected metric as an OTLP Metric. The point-type field (sum / gauge /
  histogram) is what tells a backend how the series may be aggregated."
  [m]
  (let [base (cond-> {:name (:name m)}
               (:description m) (assoc :description (:description m))
               (:unit m) (assoc :unit (:unit m)))
        temporality (get temporality-codes (:temporality m) 2)]
    (case (:type m)
      :sum (assoc base :sum {:dataPoints (mapv number-point (:data-points m))
                             :aggregationTemporality temporality
                             :isMonotonic (boolean (:monotonic? m))})
      :gauge (assoc base :gauge {:dataPoints (mapv number-point (:data-points m))})
      :histogram (assoc base :histogram
                        {:dataPoints (mapv #(histogram-point (:explicit-bounds m) %) (:data-points m))
                         :aggregationTemporality temporality}))))

(defn metrics-request
  "A collection (a sequence of {:scope … :metrics […]}) as an OTLP
  ExportMetricsServiceRequest for one resource."
  [resource collected]
  {:resourceMetrics
   [(cond-> {:resource (resource->otlp resource)
             :scopeMetrics (mapv (fn [{:keys [scope metrics]}]
                                   (cond-> {:scope (scope->otlp scope)
                                            :metrics (mapv metric->otlp metrics)}
                                     (:schema-url scope) (assoc :schemaUrl (:schema-url scope))))
                                 collected)}
      (res/schema-url resource) (assoc :schemaUrl (res/schema-url resource)))]})

;; --- logs -------------------------------------------------------------------

(defn- body-value
  "A log record's body as an AnyValue. A string stays a string; anything else is
  rendered, since a log body is meant to be read by a human and an arbitrary
  Clojure value has no faithful AnyValue shape."
  [b]
  (cond
    (nil? b) {:stringValue ""}
    (string? b) {:stringValue b}
    (or (true? b) (false? b)) {:boolValue b}
    (integer? b) {:intValue (i64 b)}
    (float? b) {:doubleValue (double b)}
    :else {:stringValue (pr-str b)}))

(defn log-record->otlp
  "One SDK log record as an OTLP LogRecord."
  [r]
  (cond-> {:observedTimeUnixNano (i64 (:observed-time-unix-nano r))
           :severityNumber (or (:severity-number r) 0)
           :body (body-value (:body r))}
    (:severity-text r) (assoc :severityText (:severity-text r))
    (and (string? (:event-name r)) (not (empty? (:event-name r))))
    (assoc :eventName (:event-name r))
    ;; timeUnixNano is when the event happened; it is optional, and omitting it
    ;; tells the backend to fall back to the observed time rather than to 1970.
    (:timestamp-unix-nano r) (assoc :timeUnixNano (i64 (:timestamp-unix-nano r)))
    (seq (:attributes r)) (assoc :attributes (key-values (:attributes r)))
    ;; The correlation that makes a log record worth sending through OTel at all.
    (:trace-id r) (assoc :traceId (:trace-id r))
    (:span-id r) (assoc :spanId (:span-id r))
    (:trace-flags r) (assoc :flags (:trace-flags r))))

(defn logs-request
  "A batch of SDK log records as an OTLP ExportLogsServiceRequest, grouped
  resource -> scope -> records."
  [records]
  {:resourceLogs
   (mapv (fn [[resource by-resource]]
           (cond-> {:resource (resource->otlp resource)
                    :scopeLogs
                    (mapv (fn [[scope by-scope]]
                            (cond-> {:scope (scope->otlp scope)
                                     :logRecords (mapv log-record->otlp by-scope)}
                              (:schema-url scope) (assoc :schemaUrl (:schema-url scope))))
                          (group-by :scope by-resource))}
             (res/schema-url resource) (assoc :schemaUrl (res/schema-url resource))))
         (group-by :resource records))})
