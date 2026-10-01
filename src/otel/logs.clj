(ns otel.logs
  "The logs API: loggers and the records they emit.

  Logs are the one OpenTelemetry signal an application does not usually call
  directly. Every language already has a logging library, and the spec's position
  is that OpenTelemetry should sit *behind* it rather than replace it — so the
  intended entry point is a bridge (see `otel.bridge.tools-logging`), and this
  namespace is the interface a bridge targets.

  What OpenTelemetry adds over the log line that was already being written is
  correlation: a record emitted inside a span carries that span's trace and span
  ids, so a backend can put the logs for one request next to the trace for that
  same request. `emit!` reads the active span for exactly that reason.

  Severity is a number, not a name. The spec fixes six ranges — TRACE 1-4,
  DEBUG 5-8, INFO 9-12, WARN 13-16, ERROR 17-20, FATAL 21-24 — so that levels
  from different logging libraries stay comparable after they are normalized."
  (:refer-clojure :exclude [name]))

(def severity-numbers
  "The base severity number of each standard level."
  {:trace 1 :debug 5 :info 9 :warn 13 :error 17 :fatal 21})

(def severity-texts
  {:trace "TRACE" :debug "DEBUG" :info "INFO"
   :warn "WARN" :error "ERROR" :fatal "FATAL"})

(defn severity-number
  "The OTel severity number for a level keyword, or 0 when unknown."
  [level]
  (get severity-numbers level 0))

(defn severity-text
  "The conventional label for a level keyword."
  [level]
  (get severity-texts level (when level (clojure.core/name level))))

(defprotocol Logger
  (emit! [logger record]
    "Emit a log record. `record` may carry :body, :severity (a level keyword),
    :severity-number, :severity-text, :event-name (a non-empty string for a
    semantically named OTel Event), :attributes, :timestamp (nanos, when the
    event happened) and :observed-timestamp (nanos, when it was collected).
    Empty or non-string event names are ignored, leaving an ordinary log
    record; telemetry input must not break the application emitting it.")
  (log-enabled? [logger level]
    "Whether a record at `level` would be recorded. Guard expensive message
    construction with it."))

(defprotocol LoggerProvider
  (get-logger* [provider scope]
    "A logger for one instrumentation scope."))

;; --- no-op ------------------------------------------------------------------

(defrecord NoopLogger []
  Logger
  (emit! [this _] this)
  (log-enabled? [_ _] false))

(def noop-logger (->NoopLogger))

(defrecord NoopLoggerProvider []
  LoggerProvider
  (get-logger* [_ _] noop-logger))

(def noop-logger-provider (->NoopLoggerProvider))
