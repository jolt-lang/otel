(ns otel.id
  "Trace and span identifier generation.

  Per the W3C Trace Context spec a trace id is 16 random bytes and a span id is 8,
  each carried on the wire as lowercase hex with no separators — 32 and 16
  characters respectively. The all-zero value of each is reserved to mean
  \"absent\" and is the only value that is invalid; generation must never produce
  it.

  The bytes come from the OS, not from `clojure.core/rand-int`. jolt seeds its
  PRNG identically in every process, so ids drawn from it are byte-for-byte the
  same on every run — every service in a fleet would report the same trace id
  and a backend would splice unrelated requests into one trace. OS entropy is
  also the cheaper of the two here: one `RAND_bytes` call costs about a
  microsecond, well under the cost of the span it identifies."
  (:require [jolt.crypto :as crypto]))

(def invalid-trace-id "00000000000000000000000000000000")
(def invalid-span-id  "0000000000000000")

(def ^:private hex-chars "0123456789abcdef")

(def ^:private hex-pairs
  "Byte value -> its two lowercase hex characters."
  (vec (for [b (range 256)]
         (str (.charAt hex-chars (bit-shift-right b 4))
              (.charAt hex-chars (bit-and b 15))))))

;; Guarantees distinct ids within the process even if the fallback below is in
;; play and the clock is coarse.
(def ^:private counter (atom 0))

(defn- fallback-bytes
  "Process-varying bytes for a host without OpenSSL. Weaker than OS entropy —
  a clock and a counter are guessable — but it keeps ids distinct across
  processes, which is the property a trace id actually depends on."
  [n]
  (let [now (try (jolt.host/wall-nanos) (catch :default _ 0))
        seed (bit-xor now (* 2654435761 (swap! counter inc)))
        out (byte-array n)]
    (dotimes [i n]
      ;; unchecked-: a byte is signed, and half these values do not fit in it
      (aset out i (unchecked-byte (bit-and (bit-xor (rand-int 256)
                                                    (unsigned-bit-shift-right seed (* 8 (mod i 8))))
                                           255))))
    out))

(def ^:private os-entropy?
  (try (crypto/random-bytes 1) true
       (catch :default _
         (binding [*out* *err*]
           (println "otel: no OS entropy (OpenSSL RAND_bytes unavailable);"
                    "trace ids fall back to clock and PRNG mixing"))
         false)))

(defn- entropy
  "`n` random bytes."
  [n]
  (if os-entropy?
    (crypto/random-bytes n)
    (fallback-bytes n)))

(defn- random-hex
  "`n` random bytes as 2n lowercase hex characters."
  [n]
  (let [b (entropy n)
        sb (StringBuilder.)]
    (dotimes [i n]
      (.append sb (nth hex-pairs (bit-and (aget b i) 255))))
    (.toString sb)))

(defn- hex-string?
  "True when `s` is exactly `n` lowercase hex characters."
  [s n]
  (and (string? s)
       (= n (count s))
       (loop [i 0]
         (if (= i n)
           true
           (let [c (int (.charAt s i))]
             (when (or (<= 48 c 57)   ; 0-9
                       (<= 97 c 102)) ; a-f
               (recur (inc i))))))))

(defn valid-trace-id?
  "True for 32 lowercase hex characters other than the reserved all-zero id."
  [s]
  (and (hex-string? s 32) (not= s invalid-trace-id)))

(defn valid-span-id?
  "True for 16 lowercase hex characters other than the reserved all-zero id."
  [s]
  (and (hex-string? s 16) (not= s invalid-span-id)))

(defn trace-id
  "A fresh random trace id: 32 lowercase hex characters, never all-zero."
  []
  ;; The retry can only fire on a 1-in-2^128 draw, but the spec makes all-zero
  ;; mean "no trace", so emitting it would silently detach the span from its trace.
  (loop []
    (let [t (random-hex 16)]
      (if (= t invalid-trace-id) (recur) t))))

(defn span-id
  "A fresh random span id: 16 lowercase hex characters, never all-zero."
  []
  (loop []
    (let [s (random-hex 8)]
      (if (= s invalid-span-id) (recur) s))))
