(ns otel.propagation
  "Context propagation over W3C Trace Context and W3C Baggage headers.

  This is what turns per-process traces into one distributed trace: the caller
  writes its active span context into outgoing request headers, and the callee
  reads them back so its spans become children of the caller's. Both sides only
  agree on the wire format, which is why a Jolt service traces correctly
  alongside Java, Go or Node services.

  Extraction is deliberately forgiving. A malformed or hostile header must leave
  the receiver with no parent — starting a fresh trace — rather than raising:
  telemetry headers arrive from outside the trust boundary, and no header should
  ever be able to fail a request."
  (:refer-clojure :exclude [extend])
  (:require [clojure.string :as str]
            [otel.baggage :as baggage]
            [otel.context :as ctx]
            [otel.id :as id]
            [otel.trace :as trace]))

(defprotocol TextMapPropagator
  (fields [propagator]
    "The carrier keys this propagator writes. A caller clearing stale values
    before injection needs this list.")
  (inject [propagator context carrier]
    "Return `carrier` with `context` written into it.")
  (extract [propagator context carrier]
    "Return `context` updated from whatever `carrier` carries."))

(defn- carrier-get
  "Look `k` up case-insensitively. HTTP header names are case-insensitive and
  real clients disagree about casing, so an exact-match lookup silently loses
  traces."
  [carrier k]
  (or (get carrier k)
      (some (fn [[ck v]]
              (when (and (string? ck) (.equalsIgnoreCase ck k)) v))
            carrier)))

;; --- W3C Trace Context ------------------------------------------------------

(def ^:private traceparent-key "traceparent")
(def ^:private tracestate-key "tracestate")

(defn- lower-hex? [s]
  (every? (fn [c] (or (and (>= (int c) (int \0)) (<= (int c) (int \9)))
                      (and (>= (int c) (int \a)) (<= (int c) (int \f)))))
          s))

(defn parse-traceparent
  "Parse a traceparent header into a span context, or nil when it is not valid.

  Version handling follows the spec: `ff` is reserved and always invalid; version
  `00` is exactly the four fields and nothing more; any higher version may append
  fields we do not understand, and must still be read for the four we do — that
  is what keeps a newer caller from breaking an older callee."
  [s]
  (when (string? s)
    (let [parts (str/split (str/trim s) #"-")]
      (when (>= (count parts) 4)
        (let [[version tid sid flags] parts]
          (when (and (= 2 (count version)) (lower-hex? version) (not= "ff" version)
                     (= 32 (count tid)) (lower-hex? tid)
                     (= 16 (count sid)) (lower-hex? sid)
                     (= 2 (count flags)) (lower-hex? flags)
                     ;; version 00 must not carry trailing fields
                     (or (not= "00" version) (= 4 (count parts)))
                     (id/valid-trace-id? tid)
                     (id/valid-span-id? sid))
            (trace/span-context {:trace-id tid
                                 :span-id sid
                                 :trace-flags (Long/parseLong flags 16)
                                 :remote? true})))))))

(defn format-traceparent
  "The traceparent header for a span context."
  [sc]
  (str "00-" (:trace-id sc) "-" (:span-id sc) "-"
       ;; Level 2 defines sampled and random-trace-id. Reserved bits must be zero
       ;; when this implementation writes version 00.
       (format "%02x" (bit-and (or (:trace-flags sc) 0)
                              (bit-or trace/flag-sampled trace/flag-random)))))

(def ^:private tracestate-max-entries
  "The spec's cap. A list longer than this is truncated rather than rejected."
  32)

(defn parse-tracestate
  "Parse a tracestate header into an ordered vector of [key value] pairs.
  Malformed entries are skipped; a wholly unparseable header yields []."
  [s]
  (if (str/blank? s)
    []
    (into []
          (comp (map str/trim)
                (remove str/blank?)
                (keep (fn [entry]
                        (let [i (str/index-of entry "=")]
                          (when (and i (pos? i))
                            (let [k (str/trim (subs entry 0 i))
                                  v (str/trim (subs entry (inc i)))]
                              (when-not (or (str/blank? k) (str/blank? v))
                                [k v]))))))
                (take tracestate-max-entries))
          (str/split s #","))))

(defn format-tracestate
  "The tracestate header for an ordered vector of [key value] pairs, or nil when
  there is nothing to send."
  [entries]
  (when (seq entries)
    (str/join "," (map (fn [[k v]] (str k "=" v)) entries))))

(defrecord TraceContextPropagator []
  TextMapPropagator
  (fields [_] [traceparent-key tracestate-key])
  (inject [_ context carrier]
    (let [sc (trace/span-context-of (trace/span-from-context context))]
      (if-not (trace/valid? sc)
        carrier
        (cond-> (assoc carrier traceparent-key (format-traceparent sc))
          (seq (:trace-state sc)) (assoc tracestate-key (format-tracestate (:trace-state sc)))))))
  (extract [_ context carrier]
    (if-let [sc (parse-traceparent (carrier-get carrier traceparent-key))]
      (let [state (parse-tracestate (carrier-get carrier tracestate-key))]
        (trace/context-with-span context
                                 (trace/non-recording-span (assoc sc :trace-state state))))
      context)))

(def trace-context
  "The W3C Trace Context propagator: traceparent + tracestate."
  (->TraceContextPropagator))

;; --- W3C Baggage ------------------------------------------------------------

(def ^:private baggage-key "baggage")

;; Everything outside this set is percent-encoded, so a value containing a comma,
;; equals or semicolon cannot be mistaken for the header's own punctuation.
(def ^:private unreserved
  (set (concat (map char (range (int \a) (inc (int \z))))
               (map char (range (int \A) (inc (int \Z))))
               (map char (range (int \0) (inc (int \9))))
               [\- \. \_ \~])))

(defn- percent-encode [s]
  (let [sb (StringBuilder.)]
    (doseq [c (str s)]
      (if (contains? unreserved c)
        (.append sb c)
        ;; UTF-8 bytes, each as %XX — a non-ASCII character is several bytes.
        (doseq [b (.getBytes (str c) "UTF-8")]
          (.append sb (format "%%%02X" (bit-and b 0xff))))))
    (.toString sb)))

(defn- percent-decode [s]
  (if-not (str/includes? s "%")
    s
    (let [n (count s)
          bytes (java.io.ByteArrayOutputStream.)]
      (loop [i 0]
        (if (>= i n)
          (.toString bytes "UTF-8")
          (let [c (.charAt s i)]
            (if (and (= c \%) (<= (+ i 3) n))
              (if-let [b (try (Integer/parseInt (subs s (inc i) (+ i 3)) 16)
                              (catch :default _ nil))]
                (do (.write bytes b) (recur (+ i 3)))
                (do (.write bytes (int c)) (recur (inc i))))
              (do (doseq [b (.getBytes (str c) "UTF-8")] (.write bytes (bit-and b 0xff)))
                  (recur (inc i))))))))))

(defrecord BaggagePropagator []
  TextMapPropagator
  (fields [_] [baggage-key])
  (inject [_ context carrier]
    (let [b (baggage/from-context context)]
      (if (baggage/empty-baggage? b)
        carrier
        (assoc carrier baggage-key
               (str/join ","
                         (map (fn [[k v]]
                                (str (percent-encode k) "=" (percent-encode v)
                                     (when-let [m (baggage/get-metadata b k)] (str ";" m))))
                              (baggage/->map b)))))))
  (extract [_ context carrier]
    (let [header (carrier-get carrier baggage-key)]
      (if (str/blank? header)
        context
        (let [entries (keep (fn [entry]
                              (let [;; metadata trails the value after a semicolon
                                    [pair metadata] (str/split (str/trim entry) #";" 2)
                                    i (some-> pair (str/index-of "="))]
                                (when (and pair i (pos? i))
                                  [(percent-decode (str/trim (subs pair 0 i)))
                                   (let [v (percent-decode (str/trim (subs pair (inc i))))]
                                     (if metadata {:value v :metadata metadata} v))])))
                            (str/split header #","))]
          (if (empty? entries)
            context
            (baggage/with-baggage context (baggage/baggage (into {} entries)))))))))

(def baggage-propagator
  "The W3C Baggage propagator."
  (->BaggagePropagator))

;; --- composite --------------------------------------------------------------

(defrecord CompositePropagator [propagators]
  TextMapPropagator
  (fields [_] (into [] (distinct) (mapcat fields propagators)))
  (inject [_ context carrier]
    (reduce (fn [c p] (inject p context c)) carrier propagators))
  (extract [_ context carrier]
    (reduce (fn [c p] (extract p c carrier)) context propagators)))

(defn composite
  "One propagator that applies several in order."
  [propagators]
  (->CompositePropagator (vec propagators)))

(def default-propagator
  "Trace context plus baggage — the spec's default configuration, and what
  almost every deployment wants."
  (composite [trace-context baggage-propagator]))

;; --- convenience ------------------------------------------------------------

(defn inject-current
  "Write the active context into `carrier` using the default propagator."
  ([carrier] (inject-current default-propagator carrier))
  ([propagator carrier] (inject propagator (ctx/current) carrier)))

(defn extract-context
  "Read `carrier` into a context using the default propagator, starting from the
  root — the usual thing to do at the edge of a service."
  ([carrier] (extract-context default-propagator carrier))
  ([propagator carrier] (extract propagator ctx/root carrier)))
