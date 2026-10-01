(ns otel.instrument.runtime
  "Runtime metrics for the Chez Scheme process.

  Chez already maintains everything worth reporting — the collector's counters
  and two clocks — and jolt exposes them through `jolt.host`. This maps them onto
  OpenTelemetry instruments. Runtime-specific measurements use the `jolt.*`
  namespace; only measurements with matching standard semantics use standard
  process or system names.

  Every instrument here is asynchronous. These are values that are cheap to read
  on demand and expensive to track continuously — there is no hook on each
  allocation, and there does not need to be — so the SDK reads them once per
  collection instead.

  The instrument kinds are chosen from what each number actually is, which is
  where a runtime integration usually goes wrong:

    heap usage             an up/down counter. It rises and falls with
                           allocation and collection.
    collection count/time  monotonic counters. They only ever increase, so a
                           backend can turn them into a rate — collections per
                           second, or fraction of time spent collecting.
    cpu time               a monotonic counter, for the same reason."
  (:require [otel.metrics :as metrics]))

(defn- observe-value
  "An observable callback reporting one value with fixed attributes."
  ([f] (observe-value f {}))
  ([f attrs] (fn [observer] (metrics/observe! observer (f) attrs))))

(defn- available?
  "Whether a host primitive can actually be read here. An older jolt may not
  expose a given jolt.host primitive; registering an instrument that can only
  ever throw would put a failing callback in every collection, so the instrument
  is left out instead and the rest still register."
  [f]
  (try (f) true (catch :default _ false)))

(defn- instrument
  "Register `build` only when `probe` can be read."
  [probe build]
  (when (available? probe) (build)))

(defn register!
  "Register the Chez runtime instruments on `meter`. Returns the instruments, so
  a caller can hold them; the SDK reads them through the meter regardless.

  Called once at startup, after the meter provider exists."
  [meter]
  (remove
    nil?
    [;; --- memory --------------------------------------------------------------
     ;; Live bytes: what survived the last collection plus what has been
     ;; allocated since. The number that answers "is this process leaking".
     (instrument #(jolt.host/bytes-allocated)
       #(metrics/observable-up-down-counter
          meter "jolt.memory.used"
          (observe-value (fn [] (jolt.host/bytes-allocated)))
          {:unit "By" :description "Bytes currently allocated on the Chez heap"}))

     ;; Bytes Chez holds from the OS. Always >= heap: it includes space the
     ;; collector has reserved but not handed out, so the gap between the two is
     ;; fragmentation and headroom rather than live data.
     (instrument #(jolt.host/current-memory-bytes)
       #(metrics/observable-up-down-counter
          meter "jolt.memory.committed"
          (observe-value (fn [] (jolt.host/current-memory-bytes)))
          {:unit "By" :description "Bytes obtained from the OS by the Chez allocator"}))

     (instrument #(jolt.host/maximum-memory-bytes)
       #(metrics/observable-gauge
          meter "jolt.memory.committed.peak"
          (observe-value (fn [] (jolt.host/maximum-memory-bytes)))
          {:unit "By" :description "Peak bytes obtained from the OS by the Chez allocator"}))

     ;; --- garbage collection --------------------------------------------------
     (instrument #(jolt.host/gc-count)
       #(metrics/observable-counter
          meter "jolt.gc.count"
          (observe-value (fn [] (jolt.host/gc-count)))
          {:unit "{collection}" :description "Collections performed since process start"}))

     ;; Seconds, because that is what the OTel conventions use for durations;
     ;; the host counter is nanoseconds.
     (instrument #(jolt.host/gc-real-nanos)
       #(metrics/observable-counter
          meter "jolt.gc.wall.time"
          (observe-value (fn [] (/ (double (jolt.host/gc-real-nanos)) 1e9)))
          {:unit "s" :description "Wall-clock time spent collecting since process start"}))

     (instrument #(jolt.host/gc-cpu-nanos)
       #(metrics/observable-counter
          meter "jolt.gc.cpu.time"
          (observe-value (fn [] (/ (double (jolt.host/gc-cpu-nanos)) 1e9)))
          {:unit "s" :description "CPU time spent collecting since process start"}))

     (instrument #(jolt.host/gc-bytes)
       #(metrics/observable-counter
          meter "jolt.gc.memory.reclaimed"
          (observe-value (fn [] (jolt.host/gc-bytes)))
          {:unit "By" :description "Bytes reclaimed by the collector since process start"}))

     ;; --- cpu -----------------------------------------------------------------
     (instrument #(jolt.host/cpu-nanos)
       #(metrics/observable-counter
          meter "jolt.cpu.time"
          (observe-value (fn [] (/ (double (jolt.host/cpu-nanos)) 1e9)))
          {:unit "s" :description "CPU time consumed by the process"}))

     (instrument #(jolt.host/real-nanos)
       #(metrics/observable-gauge
          meter "process.uptime"
          (observe-value (fn [] (/ (double (jolt.host/real-nanos)) 1e9)))
          {:unit "s" :description "Wall-clock time since process start"}))

     ;; --- host ----------------------------------------------------------------
     (instrument #(jolt.host/available-processors)
       #(metrics/observable-up-down-counter
          meter "jolt.cpu.count"
          (observe-value (fn [] (jolt.host/available-processors)))
          {:unit "{cpu}" :description "Processors available to the Jolt runtime"}))]))
