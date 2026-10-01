(ns otel.sdk.sampler
  "Sampling: the decision, taken once when a span starts, of whether to record it
  and whether to tell downstream services to record theirs.

  A sampler returns one of three decisions:

    :record-and-sample  record the span and set the sampled flag, so the whole
                        downstream trace is collected too
    :record-only        record the span locally but do not set the sampled flag —
                        used when something local wants the data but the trace as
                        a whole is not being collected
    :drop               do not record; the span becomes non-recording and costs
                        almost nothing

  The decision is made from the trace id rather than from a counter or a random
  draw so that every service in a distributed trace reaches the *same* answer
  independently. A sampler that flipped a coin per service would produce traces
  with holes in them."
  (:refer-clojure :exclude [name])
  (:require [clojure.string :as str]
            [otel.trace :as trace]))

(defprotocol Sampler
  (should-sample [sampler params]
    "Decide for a span about to start. `params` carries :parent-context,
    :trace-id, :name, :kind, :attributes and :links. Returns
    {:decision :record-and-sample|:record-only|:drop, :attributes map,
    :trace-state vector-or-nil}.")
  (description [sampler]
    "A stable string identifying the sampler and its configuration. It is
    reported in the SDK's own diagnostics, so it must include the parameters."))

(defn sampled?
  "True when the decision sets the sampled flag on the span context."
  [result]
  (= :record-and-sample (:decision result)))

(defn recording?
  "True when the decision means the span collects data."
  [result]
  (contains? #{:record-and-sample :record-only} (:decision result)))

(defn- result
  ([decision] (result decision nil))
  ([decision trace-state]
   {:decision decision :attributes {} :trace-state trace-state}))

;; --- constant samplers ------------------------------------------------------

(defrecord AlwaysOn []
  Sampler
  (should-sample [_ _] (result :record-and-sample))
  (description [_] "AlwaysOnSampler"))

(defrecord AlwaysOff []
  Sampler
  (should-sample [_ _] (result :drop))
  (description [_] "AlwaysOffSampler"))

(def always-on
  "Records and samples every span. The default, and the right choice for a
  low-traffic service or a development environment."
  (->AlwaysOn))

(def always-off
  "Drops every span. Useful to disable tracing without removing instrumentation."
  (->AlwaysOff))

;; --- ratio sampler ----------------------------------------------------------

;; Written as literals, not as (bit-shift-left 1 63): jolt's bit ops are 64-bit
;; and wrap exactly like Clojure's, so that shift yields Long/MIN_VALUE and the
;; next one yields 1. Ordinary arithmetic here is arbitrary-precision and exact.
(def ^:private two-63 9223372036854775808)
(def ^:private two-64 18446744073709551616)
(def ^:private max-signed-64 9223372036854775807)

(defn- trace-id-random-part
  "The low 64 bits of a trace id, read as a signed 64-bit integer.

  The W3C spec requires the *rightmost* portion of a trace id to be random, so
  taking the last 16 hex characters is what makes the decision uniform. Reading
  them as signed (rather than unsigned) matches the reference SDKs, so the same
  trace id decides identically in a Java, Go or Jolt service."
  [trace-id]
  ;; Parsed as two 32-bit halves: Long/parseLong rejects 16 hex digits above
  ;; Long/MAX_VALUE, which is half of all trace ids.
  (let [v (+ (* (Long/parseLong (subs trace-id 16 24) 16) 4294967296)
             (Long/parseLong (subs trace-id 24 32) 16))]
    (if (>= v two-63) (- v two-64) v)))

(defrecord TraceIdRatio [ratio upper-bound]
  Sampler
  (should-sample [_ {:keys [trace-id]}]
    (if (< (abs (trace-id-random-part trace-id)) upper-bound)
      (result :record-and-sample)
      (result :drop)))
  (description [_] (str "TraceIdRatioBased{" ratio "}")))

(defn trace-id-ratio
  "Sample the given fraction of traces, deciding from the trace id so that every
  service in a trace agrees. `ratio` is in [0.0, 1.0]."
  [ratio]
  (when-not (and (number? ratio) (<= 0.0 (double ratio) 1.0))
    (throw (ex-info "sampling ratio must be between 0.0 and 1.0" {:ratio ratio})))
  (cond
    (zero? (double ratio)) (->TraceIdRatio ratio 0)
    ;; At 1.0 the bound must strictly exceed every possible |value|, including
    ;; 2^63 — the magnitude of the one trace id that reads as Long/MIN_VALUE, which
    ;; is larger than Long/MAX_VALUE. Without this, a sampler configured to keep
    ;; everything would drop that single id.
    (== 1.0 (double ratio)) (->TraceIdRatio ratio (inc two-63))
    :else (->TraceIdRatio ratio (long (* (double ratio) max-signed-64)))))

;; --- parent-based sampler ---------------------------------------------------

(defrecord ParentBased [root remote-sampled remote-not-sampled local-sampled local-not-sampled]
  Sampler
  (should-sample [_ {:keys [parent-context] :as params}]
    (let [psc (trace/span-context-of (trace/span-from-context parent-context))
          delegate (if-not (trace/valid? psc)
                     root
                     (let [sampled (trace/sampled? psc)]
                       (if (:remote? psc)
                         (if sampled remote-sampled remote-not-sampled)
                         (if sampled local-sampled local-not-sampled))))]
      (should-sample delegate params)))
  (description [_]
    (str "ParentBased{root=" (description root)
         ",remoteParentSampled=" (description remote-sampled)
         ",remoteParentNotSampled=" (description remote-not-sampled)
         ",localParentSampled=" (description local-sampled)
         ",localParentNotSampled=" (description local-not-sampled) "}")))

(defn parent-based
  "Respect the incoming sampling decision, and use `:root` for spans that start a
  new trace. This is the sampler almost every service wants: it keeps distributed
  traces whole, because a service never overrules the decision its caller already
  made and propagated.

  Each case can be overridden individually: `:root` (default always-on),
  `:remote-parent-sampled`, `:remote-parent-not-sampled`, `:local-parent-sampled`,
  `:local-parent-not-sampled`."
  [{:keys [root remote-parent-sampled remote-parent-not-sampled
           local-parent-sampled local-parent-not-sampled]}]
  (->ParentBased (or root always-on)
                 (or remote-parent-sampled always-on)
                 (or remote-parent-not-sampled always-off)
                 (or local-parent-sampled always-on)
                 (or local-parent-not-sampled always-off)))

;; --- configuration ----------------------------------------------------------

(defn from-config
  "Build a sampler from an OTEL_TRACES_SAMPLER-style name and argument:
  always_on, always_off, traceidratio, parentbased_always_on,
  parentbased_always_off, parentbased_traceidratio. Returns nil for an
  unrecognized name, so the caller can fall back to its default."
  [sampler-name arg]
  (let [ratio (fn [] (if (str/blank? arg) 1.0 (Double/parseDouble arg)))]
    (case (some-> sampler-name str/trim str/lower-case)
      "always_on" always-on
      "always_off" always-off
      "traceidratio" (trace-id-ratio (ratio))
      "parentbased_always_on" (parent-based {:root always-on})
      "parentbased_always_off" (parent-based {:root always-off})
      "parentbased_traceidratio" (parent-based {:root (trace-id-ratio (ratio))})
      nil)))

(def default-sampler
  "The spec's default: follow the parent, and sample every new trace."
  (parent-based {:root always-on}))
