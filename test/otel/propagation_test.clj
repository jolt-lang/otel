(ns otel.propagation-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.baggage :as baggage]
            [otel.context :as ctx]
            [otel.propagation :as prop]
            [otel.trace :as trace]))

(def tid "0af7651916cd43dd8448eb211c80319c")
(def sid "b7ad6b7169203331")
(def header (str "00-" tid "-" sid "-01"))

(defn- ctx-with [sc]
  (trace/context-with-span ctx/root (trace/non-recording-span sc)))

;; --- traceparent extraction -------------------------------------------------

(deftest extracts-a-valid-traceparent
  (let [c (prop/extract prop/trace-context ctx/root {"traceparent" header})
        sc (trace/span-context-of (trace/span-from-context c))]
    (is (= tid (:trace-id sc)))
    (is (= sid (:span-id sc)))
    (is (trace/sampled? sc))
    (testing "an extracted context is by definition remote"
      (is (:remote? sc)))))

(deftest extracts-an-unsampled-traceparent
  (let [c (prop/extract prop/trace-context ctx/root {"traceparent" (str "00-" tid "-" sid "-00")})
        sc (trace/span-context-of (trace/span-from-context c))]
    (is (trace/valid? sc))
    (is (not (trace/sampled? sc)))))

(deftest header-lookup-is-case-insensitive
  (testing "HTTP header names are case-insensitive, and real clients vary"
    (doseq [k ["traceparent" "Traceparent" "TRACEPARENT"]]
      (let [c (prop/extract prop/trace-context ctx/root {k header})]
        (is (trace/valid? (trace/span-context-of (trace/span-from-context c)))
            (str "failed for " k))))))

(deftest rejects-malformed-traceparents
  (doseq [bad ["" "garbage"
               ;; wrong field count
               (str "00-" tid "-" sid)
               ;; ff is the reserved invalid version
               (str "ff-" tid "-" sid "-01")
               ;; all-zero ids are the "absent" value, never a real parent
               (str "00-00000000000000000000000000000000-" sid "-01")
               (str "00-" tid "-0000000000000000-01")
               ;; wrong lengths
               (str "00-" (subs tid 1) "-" sid "-01")
               (str "00-" tid "-" (subs sid 1) "-01")
               ;; uppercase is not valid hex per the spec
               (str "00-" (.toUpperCase tid) "-" sid "-01")
               ;; non-hex
               (str "00-" (apply str (repeat 32 "g")) "-" sid "-01")]]
    (let [c (prop/extract prop/trace-context ctx/root {"traceparent" bad})]
      (is (not (trace/valid? (trace/span-context-of (trace/span-from-context c))))
          (str "should have rejected: " (pr-str bad))))))

(deftest a-future-version-with-extra-fields-is-accepted
  (testing "spec: an unknown version must still be parsed for its first four fields
            rather than dropped, so a newer caller does not break the trace"
    (let [c (prop/extract prop/trace-context ctx/root
                          {"traceparent" (str "01-" tid "-" sid "-01-extra")})
          sc (trace/span-context-of (trace/span-from-context c))]
      (is (trace/valid? sc))
      (is (= tid (:trace-id sc))))))

(deftest version-00-rejects-trailing-data
  (testing "spec: version 00 is exactly 55 characters"
    (let [c (prop/extract prop/trace-context ctx/root
                          {"traceparent" (str "00-" tid "-" sid "-01-extra")})]
      (is (not (trace/valid? (trace/span-context-of (trace/span-from-context c))))))))

(deftest missing-traceparent-leaves-the-context-alone
  (is (= ctx/root (prop/extract prop/trace-context ctx/root {}))))

;; --- tracestate -------------------------------------------------------------

(deftest extracts-tracestate
  (let [c (prop/extract prop/trace-context ctx/root
                        {"traceparent" header "tracestate" "vendor1=abc,vendor2=def"})
        sc (trace/span-context-of (trace/span-from-context c))]
    (is (= [["vendor1" "abc"] ["vendor2" "def"]] (:trace-state sc)))))

(deftest tracestate-preserves-order
  (testing "order is significant — the leftmost entry is the most recent writer"
    (let [c (prop/extract prop/trace-context ctx/root
                          {"traceparent" header "tracestate" "c=3,a=1,b=2"})]
      (is (= ["c" "a" "b"] (map first (:trace-state (trace/span-context-of (trace/span-from-context c)))))))))

(deftest malformed-tracestate-does-not-invalidate-the-trace
  (testing "spec: an unparseable tracestate is discarded, but the traceparent stands"
    (let [c (prop/extract prop/trace-context ctx/root
                          {"traceparent" header "tracestate" "=======" })
          sc (trace/span-context-of (trace/span-from-context c))]
      (is (trace/valid? sc))
      (is (= [] (:trace-state sc))))))

;; --- injection --------------------------------------------------------------

(deftest injects-a-traceparent
  (let [sc (trace/span-context {:trace-id tid :span-id sid :sampled? true})
        carrier (prop/inject prop/trace-context (ctx-with sc) {})]
    (is (= header (get carrier "traceparent")))))

(deftest injects-unsampled-flags
  (let [sc (trace/span-context {:trace-id tid :span-id sid :sampled? false})
        carrier (prop/inject prop/trace-context (ctx-with sc) {})]
    (is (= (str "00-" tid "-" sid "-00") (get carrier "traceparent")))))

(deftest preserves-the-w3c-random-trace-id-flag
  (let [incoming (str "00-" tid "-" sid "-02")
        context (prop/extract prop/trace-context ctx/root {"traceparent" incoming})
        sc (trace/span-context-of (trace/span-from-context context))
        carrier (prop/inject prop/trace-context context {})]
    (is (= trace/flag-random (:trace-flags sc)))
    (is (not (trace/sampled? sc)))
    (is (= incoming (get carrier "traceparent")))))

(deftest clears-reserved-trace-flags-when-writing-version-00
  (let [sc (trace/span-context {:trace-id tid :span-id sid :trace-flags 255})
        carrier (prop/inject prop/trace-context (ctx-with sc) {})]
    (is (= (str "00-" tid "-" sid "-03")
           (get carrier "traceparent")))))

(deftest injects-tracestate-when-present
  (let [sc (trace/span-context {:trace-id tid :span-id sid :sampled? true
                                :trace-state [["a" "1"] ["b" "2"]]})
        carrier (prop/inject prop/trace-context (ctx-with sc) {})]
    (is (= "a=1,b=2" (get carrier "tracestate")))))

(deftest omits-tracestate-when-empty
  (let [sc (trace/span-context {:trace-id tid :span-id sid :sampled? true})
        carrier (prop/inject prop/trace-context (ctx-with sc) {})]
    (is (not (contains? carrier "tracestate")))))

(deftest injecting-an-invalid-context-writes-nothing
  (is (= {} (prop/inject prop/trace-context ctx/root {}))))

(deftest inject-extract-round-trips
  (let [sc (trace/span-context {:trace-id tid :span-id sid :sampled? true
                                :trace-state [["v" "x"]]})
        carrier (prop/inject prop/trace-context (ctx-with sc) {})
        back (trace/span-context-of
               (trace/span-from-context (prop/extract prop/trace-context ctx/root carrier)))]
    (is (= tid (:trace-id back)))
    (is (= sid (:span-id back)))
    (is (trace/sampled? back))
    (is (= [["v" "x"]] (:trace-state back)))))

;; --- baggage ----------------------------------------------------------------

(deftest baggage-round-trips
  (let [c (baggage/with-baggage ctx/root (baggage/baggage {"user" "alice" "tier" "gold"}))
        carrier (prop/inject prop/baggage-propagator c {})
        back (baggage/from-context (prop/extract prop/baggage-propagator ctx/root carrier))]
    (is (= "alice" (baggage/get-value back "user")))
    (is (= "gold" (baggage/get-value back "tier")))))

(deftest baggage-percent-encodes-values
  (testing "a value containing a comma or equals must survive the header syntax"
    (let [c (baggage/with-baggage ctx/root (baggage/baggage {"k" "a,b=c d"}))
          carrier (prop/inject prop/baggage-propagator c {})
          back (baggage/from-context (prop/extract prop/baggage-propagator ctx/root carrier))]
      (is (not (clojure.string/includes? (get carrier "baggage") "a,b")))
      (is (= "a,b=c d" (baggage/get-value back "k"))))))

(deftest baggage-ignores-malformed-entries
  (let [c (prop/extract prop/baggage-propagator ctx/root {"baggage" "novalue,good=1"})
        b (baggage/from-context c)]
    (is (= "1" (baggage/get-value b "good")))
    (is (nil? (baggage/get-value b "novalue")))))

(deftest empty-baggage-injects-nothing
  (is (= {} (prop/inject prop/baggage-propagator ctx/root {}))))

(deftest baggage-operations
  (let [b (-> (baggage/baggage) (baggage/put "a" "1") (baggage/put "b" "2"))]
    (is (= "1" (baggage/get-value b "a")))
    (is (= #{"a" "b"} (set (keys (baggage/->map b)))))
    (is (nil? (baggage/get-value (baggage/remove-key b "a") "a")))))

;; --- composite --------------------------------------------------------------

(deftest composite-propagator-handles-both-signals
  (let [sc (trace/span-context {:trace-id tid :span-id sid :sampled? true})
        c (-> (ctx-with sc)
              (baggage/with-baggage (baggage/baggage {"user" "alice"})))
        carrier (prop/inject prop/default-propagator c {})]
    (is (= header (get carrier "traceparent")))
    (is (clojure.string/includes? (get carrier "baggage") "user"))
    (let [back (prop/extract prop/default-propagator ctx/root carrier)]
      (is (= tid (:trace-id (trace/span-context-of (trace/span-from-context back)))))
      (is (= "alice" (baggage/get-value (baggage/from-context back) "user"))))))

(deftest composite-reports-its-fields
  (is (= #{"traceparent" "tracestate" "baggage"} (set (prop/fields prop/default-propagator)))))
