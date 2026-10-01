(ns otel.sdk.exemplar-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.sdk.clock :as clock]
            [otel.sdk.exemplar :as exemplar]
            [otel.trace :as trace]))

(def valid-context
  (trace/span-context {:trace-id "11111111111111111111111111111111"
                       :span-id "2222222222222222"
                       :sampled? true}))

(def test-clock
  (clock/fake-clock {:wall 1 :mono 1}))

(deftest filters-have-distinct-eligibility
  (testing "AlwaysOff rejects without consulting span validity"
    (is (nil? (exemplar/eligible-context :always-off))))
  (testing "AlwaysOn admits a measurement even outside a valid span"
    (is (map? (exemplar/eligible-context :always-on))))
  (testing "TraceBased admits only a sampled active span"
    (is (nil? (exemplar/eligible-context :trace-based)))
    (trace/with-current-span
      (trace/non-recording-span
        (trace/span-context {:trace-id "00000000000000000000000000000000"
                             :span-id "0000000000000000"
                             :sampled? true}))
      (is (nil? (exemplar/eligible-context :trace-based))))
    (trace/with-current-span (trace/non-recording-span valid-context)
      (is (= valid-context
             (exemplar/eligible-context :trace-based))))))

(deftest invalid-filter-is-rejected-at-configuration
  (is (thrown? Exception (exemplar/check-filter :sometimes))))

(deftest a-measurement-keeps-only-filtered-attributes
  (let [e (exemplar/measurement valid-context
                                7
                                {"route" "/a" "request.id" "r-1"}
                                {"route" "/a"}
                                123)]
    (is (= 7 (:value e)))
    (is (= 123 (:time-unix-nano e)))
    (is (= {"request.id" "r-1"} (:filtered-attributes e)))
    (is (= (:trace-id valid-context) (:trace-id e)))
    (is (= (:span-id valid-context) (:span-id e)))))

(deftest always-on-outside-a-span-omits-invalid-ids
  (let [e (exemplar/measurement (exemplar/eligible-context :always-on)
                                1 {} {} 10)]
    (is (nil? (:trace-id e)))
    (is (nil? (:span-id e)))))

(deftest fixed-reservoir-uses-uniform-algorithm-r
  (let [draws (atom [2 0])
        random (fn [_] (let [v (first @draws)] (swap! draws subvec 1) v))
        r (-> (exemplar/reservoir :sum nil 2)
              (exemplar/offer 1 trace/invalid-span-context {} {} test-clock random)
              (exemplar/offer 2 trace/invalid-span-context {} {} test-clock random)
              ;; seen=2, draw 2: candidate is outside the two slots
              (exemplar/offer 3 trace/invalid-span-context {} {} test-clock random)
              ;; seen=3, draw 0: replace slot zero
              (exemplar/offer 4 trace/invalid-span-context {} {} test-clock random))]
    (is (= [4 2] (mapv :value (exemplar/collect r))))
    (is (= 4 (:seen r)))))

(deftest histogram-reservoir-is-aligned-and-bounded
  (let [random (constantly 0)
        r (reduce #(exemplar/offer %1 %2 trace/invalid-span-context {} {} test-clock random)
                  (exemplar/reservoir :histogram [10.0 100.0] 1)
                  [1 5 50 500])]
    (is (= [5 50 500] (mapv :value (exemplar/collect r))))
    (is (= [2 1 1] (:seen r)))
    (testing "one slot per histogram bucket is the hard bound"
      (is (= 3 (count (:slots r)))))))

(deftest collection-reset-clears-samples-and-seen-counts
  (let [r (-> (exemplar/reservoir :sum nil 1)
              (exemplar/offer 1 trace/invalid-span-context {} {} test-clock (constantly 0)))
        reset (exemplar/reset r)]
    (is (= [] (exemplar/collect reset)))
    (is (= 0 (:seen reset)))))

(deftest a-rejected-offer-does-not-read-the-clock-or-build-an-exemplar
  (let [wall-reads (atom 0)
        c (reify clock/Clock
            (wall-nanos [_] (swap! wall-reads inc))
            (mono-nanos [_] 0))
        accepted (exemplar/offer (exemplar/reservoir :sum nil 1)
                                 1 trace/invalid-span-context {} {} c (constantly 0))
        rejected (exemplar/offer accepted
                                 2 trace/invalid-span-context {} {} c (constantly 1))]
    (is (= 1 @wall-reads))
    (is (= [1] (mapv :value (exemplar/collect rejected))))))
