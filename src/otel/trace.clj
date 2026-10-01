(ns otel.trace
  "The tracing API: span contexts, the Span and Tracer protocols, and the plumbing
  that makes a span the active one.

  This namespace is the surface instrumentation code writes against, and it is
  deliberately usable with no SDK installed. Every operation has a working
  no-op implementation, so a library can instrument itself unconditionally and an
  application that never configures an SDK pays only a protocol dispatch. That is
  also why `current-span` returns an invalid span rather than nil: calling code
  never needs a nil check, and never needs to ask whether tracing is on."
  (:refer-clojure :exclude [name])
  (:require [otel.context :as ctx]
            [otel.id :as id]))

;; --- span context -----------------------------------------------------------

(def flag-sampled
  "Bit 0 of trace-flags: the sampled flag, as defined by W3C Trace Context."
  1)

(def flag-random
  "Bit 1 of trace-flags: the trace id has W3C Level 2 randomness."
  2)

(defrecord SpanContext [trace-id span-id trace-flags trace-state remote?])

(defn span-context
  "Build a span context. `:sampled?` is a convenience for setting bit 0 of
  `:trace-flags`; pass `:trace-flags` directly when carrying the W3C random flag.
  `:trace-state` is an ordered vector of [key value] pairs."
  [{:keys [trace-id span-id trace-flags trace-state sampled? remote?]}]
  (->SpanContext trace-id
                 span-id
                 (or trace-flags (if sampled? flag-sampled 0))
                 (or trace-state [])
                 (boolean remote?)))

(def invalid-span-context
  "The span context meaning \"no span\": both ids all-zero."
  (span-context {:trace-id id/invalid-trace-id :span-id id/invalid-span-id}))

(defn sampled?
  "True when the context's sampled flag is set. Reads only bit 0, so unknown
  flags never influence the answer."
  [sc]
  (and sc (pos? (bit-and (or (:trace-flags sc) 0) flag-sampled))))

(defn valid?
  "True when both ids are well-formed and neither is the reserved all-zero value."
  [sc]
  (boolean
    (and sc
         (id/valid-trace-id? (:trace-id sc))
         (id/valid-span-id? (:span-id sc)))))

;; --- status and kind --------------------------------------------------------

(def status-codes
  "Span status codes. :unset is the default; :ok is set only by explicit caller
  intent; :error marks a failed operation."
  [:unset :ok :error])

(def span-kinds
  "Span kinds. :internal is the default; :server/:client and :producer/:consumer
  mark the two sides of a remote call and an async handoff respectively."
  [:internal :server :client :producer :consumer])

;; --- span -------------------------------------------------------------------

(defprotocol Span
  "A single operation within a trace. All mutators return the span so they can be
  threaded, and all are safe to call on a non-recording span."
  (span-context-of [span]
    "The span's context — its identity on the wire.")
  (recording? [span]
    "True when the span is collecting data. False for a sampled-out or ended span;
    guard expensive attribute computation with it.")
  (set-attribute! [span k v]
    "Set one attribute.")
  (set-attributes! [span attrs]
    "Set several attributes from a map.")
  (add-event! [span name] [span name attrs] [span name attrs timestamp-nanos]
    "Record a timestamped event on the span.")
  (add-link! [span linked-span-context] [span linked-span-context attrs]
    "Link this span to another, typically in a different trace.")
  (set-status! [span code] [span code description]
    "Set the span's status. `code` is one of :unset, :ok, :error.")
  (update-name! [span name]
    "Rename the span. Useful when the good name is only known after the work runs
    (a routed HTTP path, say).")
  (record-exception! [span throwable] [span throwable attrs]
    "Record an exception as a span event. Does not set the status — a recorded
    exception is not necessarily a failed operation.")
  (end! [span] [span timestamp-nanos]
    "End the span. Later mutations are ignored. Returns nil."))

;; A span that carries a context but records nothing: the result of sampling out,
;; of extracting a remote context with no SDK installed, or of the invalid span.
;; It must still return its context so downstream propagation keeps working.
(defrecord NonRecordingSpan [sc]
  Span
  (span-context-of [_] sc)
  (recording? [_] false)
  (set-attribute! [this _ _] this)
  (set-attributes! [this _] this)
  (add-event! [this _] this)
  (add-event! [this _ _] this)
  (add-event! [this _ _ _] this)
  (add-link! [this _] this)
  (add-link! [this _ _] this)
  (set-status! [this _] this)
  (set-status! [this _ _] this)
  (update-name! [this _] this)
  (record-exception! [this _] this)
  (record-exception! [this _ _] this)
  (end! [_] nil)
  (end! [_ _] nil))

(defn non-recording-span
  "A span that propagates `sc` but records nothing."
  [sc]
  (->NonRecordingSpan sc))

(def invalid-span
  "The span meaning \"no span\" — what `current-span` returns outside any span."
  (non-recording-span invalid-span-context))

;; --- tracer -----------------------------------------------------------------

(defprotocol Tracer
  "Creates spans for one instrumentation scope."
  (start-span* [tracer name opts]
    "Start and return a span without making it current. `opts` may carry
    :kind, :attributes, :links, :parent (a context) and :start-timestamp."))

(defn start-span
  "Start a span without making it current. The caller is responsible for calling
  `end!` — prefer `with-span`, which cannot leak an unended span."
  ([tracer name] (start-span* tracer name {}))
  ([tracer name opts] (start-span* tracer name opts)))

;; A tracer that hands back non-recording spans: what the API yields with no SDK.
;; It preserves the parent context so an extracted remote trace still propagates.
(defrecord NoopTracer []
  Tracer
  (start-span* [_ _ opts]
    (let [parent (:parent opts (ctx/current))
          psc (some-> (ctx/get-value parent ::span) span-context-of)]
      (non-recording-span (if (valid? psc) psc invalid-span-context)))))

(def noop-tracer (->NoopTracer))

;; --- span <-> context -------------------------------------------------------

(defn context-with-span
  "A context with `span` as its active span."
  [context span]
  (ctx/with-value context ::span span))

(defn span-from-context
  "The active span in `context`, or the invalid span when there is none."
  [context]
  (or (ctx/get-value context ::span) invalid-span))

(defn current-span
  "The span active on this thread, or the invalid span."
  []
  (span-from-context (ctx/current)))

(defn current-span-context
  "The span context active on this thread, or the invalid span context."
  []
  (span-context-of (current-span)))

(defmacro with-current-span
  "Run `body` with `span` active. Does not end the span."
  [span & body]
  `(ctx/with-context (context-with-span (ctx/current) ~span) ~@body))

;; --- the instrumentation entry point ----------------------------------------

(defmacro with-span
  "Start a span, make it current for `body`, and end it on the way out.

      (with-span [sp tracer \"checkout\" {:kind :server}]
        (trace/set-attribute! sp :order.id id)
        ...)

  The span is ended even when `body` throws, and a thrown exception is recorded
  on the span and its status set to :error before the exception propagates —
  which is the behavior every OpenTelemetry SDK gives this pattern.

  The binding symbol is required, as in `with-open`; bind `_` when the body has
  no use for the span. Making it optional would be ambiguous, since a span name
  is usually an expression rather than a literal and could not be told apart from
  a tracer."
  {:arglists '([[span-binding tracer name] & body]
               [[span-binding tracer name opts] & body])}
  [binding-form & body]
  (when-not (<= 3 (count binding-form) 4)
    (throw (ex-info "with-span expects [span-binding tracer name] or [span-binding tracer name opts]"
                    {:form binding-form})))
  (let [[sym tracer nm opts] binding-form]
    `(let [~sym (start-span ~tracer ~nm ~(or opts {}))]
       (try
         (with-current-span ~sym ~@body)
         (catch :default e#
           (record-exception! ~sym e#)
           (set-status! ~sym :error (or (ex-message e#) (str e#)))
           (throw e#))
         (finally
           (end! ~sym))))))
