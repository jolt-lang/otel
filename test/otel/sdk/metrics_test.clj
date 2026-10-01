(ns otel.sdk.metrics-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.instrument.runtime :as runtime]
            [otel.metrics :as api]
            [otel.resource :as res]
            [otel.sdk.clock :as clock]
            [otel.sdk.metrics :as sdk]))

(defn- setup
  ([] (setup {}))
  ([opts]
   (let [provider (sdk/meter-provider (merge {:resource res/empty-resource
                                              :clock (clock/fake-clock {:wall 1000 :mono 0})}
                                             opts))]
     {:provider provider :meter (sdk/get-meter provider {:name "test"})})))

(defn- metric-named [provider nm]
  (->> (sdk/collect! provider) (mapcat :metrics) (filter #(= nm (:name %))) first))

(defn- point-for [metric attrs]
  (first (filter #(= attrs (:attributes %)) (:data-points metric))))

;; --- counters ---------------------------------------------------------------

(deftest counter-sums-measurements
  (let [{:keys [provider meter]} (setup)
        c (api/counter meter "requests" {:unit "{request}" :description "served"})]
    (api/add! c 1)
    (api/add! c 4)
    (let [m (metric-named provider "requests")]
      (is (= :sum (:type m)))
      (is (:monotonic? m))
      (is (= "{request}" (:unit m)))
      (is (= 5 (:value (point-for m {})))))))

(deftest counter-separates-attribute-sets
  (let [{:keys [provider meter]} (setup)
        c (api/counter meter "requests")]
    (api/add! c 1 {:route "/a"})
    (api/add! c 2 {:route "/a"})
    (api/add! c 5 {:route "/b"})
    (let [m (metric-named provider "requests")]
      (is (= 2 (count (:data-points m))))
      (is (= 3 (:value (point-for m {"route" "/a"}))))
      (is (= 5 (:value (point-for m {"route" "/b"})))))))

(deftest counter-ignores-a-negative-add
  (testing "a monotonic counter that decreased would be unrepresentable downstream"
    (let [{:keys [provider meter]} (setup)
          c (api/counter meter "requests")]
      (api/add! c 5)
      (api/add! c -3)
      (is (= 5 (:value (point-for (metric-named provider "requests") {})))))))

(deftest up-down-counter-accepts-negatives
  (let [{:keys [provider meter]} (setup)
        c (api/up-down-counter meter "queue.depth")]
    (api/add-delta! c 5)
    (api/add-delta! c -2)
    (let [m (metric-named provider "queue.depth")]
      (is (= :sum (:type m)))
      (is (not (:monotonic? m)))
      (is (= 3 (:value (point-for m {})))))))

;; --- gauges -----------------------------------------------------------------

(deftest gauge-keeps-the-latest-value
  (let [{:keys [provider meter]} (setup)
        g (api/gauge meter "temperature")]
    (api/set-value! g 10)
    (api/set-value! g 20)
    (let [m (metric-named provider "temperature")]
      (is (= :gauge (:type m)))
      (is (= 20 (:value (point-for m {}))))
      (testing "a gauge point describes an instant, so it carries no start time"
        (is (nil? (:start-time-unix-nano (point-for m {}))))))))

;; --- histograms -------------------------------------------------------------

(deftest histogram-aggregates-a-distribution
  (let [{:keys [provider meter]} (setup)
        h (api/histogram meter "latency" {:unit "ms" :boundaries [10.0 100.0]})]
    (doseq [v [5 50 500 50]] (api/record! h v))
    (let [m (metric-named provider "latency")
          p (point-for m {})]
      (is (= :histogram (:type m)))
      (is (= [10.0 100.0] (:explicit-bounds m)))
      (is (= 4 (:count p)))
      (is (= 605.0 (:sum p)))
      (is (= 5.0 (:min p)))
      (is (= 500.0 (:max p)))
      (testing "buckets are (-inf,10], (10,100], (100,+inf)"
        (is (= [1 2 1] (:bucket-counts p)))))))

(deftest histogram-boundary-values-fall-in-the-lower-bucket
  (testing "buckets are closed on the upper bound, per the spec"
    (let [{:keys [provider meter]} (setup)
          h (api/histogram meter "h" {:boundaries [10.0]})]
      (api/record! h 10)
      (is (= [1 0] (:bucket-counts (point-for (metric-named provider "h") {})))))))

(deftest histogram-uses-default-boundaries
  (let [{:keys [provider meter]} (setup)
        h (api/histogram meter "h")]
    (api/record! h 1)
    (is (= sdk/default-boundaries (:explicit-bounds (metric-named provider "h"))))))

;; --- asynchronous instruments -----------------------------------------------

(deftest observable-gauge-reads-on-collection
  (let [{:keys [provider meter]} (setup)
        current (atom 7)]
    (api/observable-gauge meter "heap" (fn [obs] (api/observe! obs @current)))
    (is (= 7 (:value (point-for (metric-named provider "heap") {}))))
    (reset! current 9)
    (testing "the callback runs again on the next collection"
      (is (= 9 (:value (point-for (metric-named provider "heap") {})))))))

(deftest observable-counter-is-a-monotonic-sum
  (let [{:keys [provider meter]} (setup)]
    (api/observable-counter meter "total" (fn [obs] (api/observe! obs 42)))
    (let [m (metric-named provider "total")]
      (is (= :sum (:type m)))
      (is (:monotonic? m))
      (is (= 42 (:value (point-for m {})))))))

(deftest observable-can-report-several-attribute-sets
  (let [{:keys [provider meter]} (setup)]
    (api/observable-gauge meter "by-pool"
                          (fn [obs]
                            (api/observe! obs 1 {:pool "a"})
                            (api/observe! obs 2 {:pool "b"})))
    (let [m (metric-named provider "by-pool")]
      (is (= 1 (:value (point-for m {"pool" "a"}))))
      (is (= 2 (:value (point-for m {"pool" "b"})))))))

(deftest a-throwing-callback-does-not-break-collection
  (testing "one bad instrument must not take out every other metric"
    (let [{:keys [provider meter]} (setup)]
      (api/observable-gauge meter "bad" (fn [_] (throw (ex-info "boom" {}))))
      (api/observable-gauge meter "good" (fn [obs] (api/observe! obs 1)))
      (is (= 1 (:value (point-for (metric-named provider "good") {})))))))

(deftest an-attribute-set-that-stops-being-reported-disappears
  (let [{:keys [provider meter]} (setup)
        report-b? (atom true)]
    (api/observable-gauge meter "g"
                          (fn [obs]
                            (api/observe! obs 1 {:k "a"})
                            (when @report-b? (api/observe! obs 2 {:k "b"}))))
    (is (= 2 (count (:data-points (metric-named provider "g")))))
    (reset! report-b? false)
    (is (= 1 (count (:data-points (metric-named provider "g")))))))

;; --- temporality ------------------------------------------------------------

(deftest cumulative-temporality-keeps-running-totals
  (let [{:keys [provider meter]} (setup)
        c (api/counter meter "n")]
    (api/add! c 1)
    (is (= 1 (:value (point-for (metric-named provider "n") {}))))
    (api/add! c 1)
    (testing "the second collection reports the total, not the increment"
      (is (= 2 (:value (point-for (metric-named provider "n") {})))))))

(deftest delta-temporality-resets-after-collection
  (let [{:keys [provider meter]} (setup {:temporality :delta})
        c (api/counter meter "n")]
    (api/add! c 3)
    (is (= 3 (:value (point-for (metric-named provider "n") {}))))
    (api/add! c 2)
    (testing "the second collection reports only what happened since the first"
      (is (= 2 (:value (point-for (metric-named provider "n") {})))))))

(deftest async-instruments-stay-cumulative-under-delta
  (testing "an absolute reading has no delta to compute, so its temporality is
            cumulative regardless of configuration"
    (let [{:keys [provider meter]} (setup {:temporality :delta})]
      (api/observable-counter meter "total" (fn [obs] (api/observe! obs 42)))
      (is (= :cumulative (:temporality (metric-named provider "total"))))
      (is (= 42 (:value (point-for (metric-named provider "total") {}))))
      (is (= 42 (:value (point-for (metric-named provider "total") {})))))))

;; --- scoping ----------------------------------------------------------------

(deftest metrics-are-grouped-by-scope
  (let [provider (sdk/meter-provider {:resource res/empty-resource})
        m1 (sdk/get-meter provider {:name "lib-a"})
        m2 (sdk/get-meter provider {:name "lib-b"})]
    (api/add! (api/counter m1 "a") 1)
    (api/add! (api/counter m2 "b") 1)
    (let [collected (sdk/collect! provider)]
      (is (= #{"lib-a" "lib-b"} (set (map #(get-in % [:scope :name]) collected)))))))

;; --- no-op API --------------------------------------------------------------

(deftest the-api-works-without-an-sdk
  (testing "instrumentation must be safe to write before anyone configures metrics"
    (let [m api/noop-meter]
      (is (some? (api/add! (api/counter m "c") 1)))
      (is (some? (api/record! (api/histogram m "h") 1.0)))
      (is (some? (api/set-value! (api/gauge m "g") 1)))
      (is (some? (api/observable-gauge m "og" (fn [_] nil)))))))

;; --- Chez runtime instrumentation -------------------------------------------

(deftest runtime-instruments-read-the-chez-collector
  (let [{:keys [provider meter]} (setup)]
    (runtime/register! meter)
    (let [by-name (into {} (map (juxt :name identity) (mapcat :metrics (sdk/collect! provider))))]
      (testing "the exported surface is exactly the reviewed standard/custom mapping"
        (is (= #{"jolt.memory.used"
                 "jolt.memory.committed"
                 "jolt.memory.committed.peak"
                 "jolt.gc.count"
                 "jolt.gc.wall.time"
                 "jolt.gc.cpu.time"
                 "jolt.gc.memory.reclaimed"
                 "jolt.cpu.time"
                 "process.uptime"
                 "jolt.cpu.count"}
               (set (keys by-name)))))
      (testing "every kind, unit, and monotonicity matches the reviewed mapping"
        (doseq [[name expected]
                {"jolt.memory.used" {:type :sum :unit "By" :monotonic? false}
                 "jolt.memory.committed" {:type :sum :unit "By" :monotonic? false}
                 "jolt.memory.committed.peak" {:type :gauge :unit "By"}
                 "jolt.gc.count" {:type :sum :unit "{collection}" :monotonic? true}
                 "jolt.gc.wall.time" {:type :sum :unit "s" :monotonic? true}
                 "jolt.gc.cpu.time" {:type :sum :unit "s" :monotonic? true}
                 "jolt.gc.memory.reclaimed" {:type :sum :unit "By" :monotonic? true}
                 "jolt.cpu.time" {:type :sum :unit "s" :monotonic? true}
                 "process.uptime" {:type :gauge :unit "s"}
                 "jolt.cpu.count" {:type :sum :unit "{cpu}" :monotonic? false}}]
          (is (= expected (select-keys (get by-name name)
                                       [:type :unit :monotonic?]))
              name)))
      (testing "live memory is a non-monotonic sum and peak memory is a gauge"
        (doseq [name ["jolt.memory.used" "jolt.memory.committed"]]
          (is (= :sum (:type (get by-name name))))
          (is (false? (:monotonic? (get by-name name))))
          (is (pos? (:value (point-for (get by-name name) {})))))
        (is (= :gauge (:type (get by-name "jolt.memory.committed.peak")))))
      (testing "collection totals are monotonic counters, so a backend can rate them"
        (let [gc (get by-name "jolt.gc.count")]
          (is (= :sum (:type gc)))
          (is (:monotonic? gc))
          (is (>= (:value (point-for gc {})) 0))))
      (testing "durations are reported in seconds, per the OTel conventions"
        (is (= "s" (:unit (get by-name "jolt.gc.wall.time"))))
        (is (>= (:value (point-for (get by-name "jolt.cpu.time") {})) 0.0))
        (is (= :gauge (:type (get by-name "process.uptime"))))
        (is (pos? (:value (point-for (get by-name "process.uptime") {})))))
      (testing "runtime-visible cpu count is a non-monotonic sum"
        (let [cpu-count (get by-name "jolt.cpu.count")]
          (is (= :sum (:type cpu-count)))
          (is (false? (:monotonic? cpu-count)))
          (is (pos? (:value (point-for cpu-count {})))))))))

;; Not "allocating makes the number go up": the metric reports bytes live on the
;; Chez heap, so a collection between two reads can leave it lower than it
;; started no matter what the test retains. What the instrument owes us is that each
;; read reflects the host counter at that moment rather than a cached constant.
(deftest runtime-memory-used-tracks-real-allocation
  (let [{:keys [provider meter]} (setup)
        read-memory #(:value (point-for (metric-named provider "jolt.memory.used") {}))
        ;; reading the metric allocates, so allow a slack rather than equality
        tracks? (fn [reading host] (< (abs (- reading host)) (max 2000000 (* 0.05 host))))]
    (runtime/register! meter)
    (let [host-1 (jolt.host/bytes-allocated)
          reading-1 (read-memory)
          keep (into [] (map #(str "padpadpadpad" %) (range 100000)))
          host-2 (jolt.host/bytes-allocated)
          reading-2 (read-memory)]
      (is (= 100000 (count keep)))
      (is (pos? reading-1))
      (is (tracks? reading-1 host-1) "the metric must report the host counter, not a constant")
      (is (tracks? reading-2 host-2) "and must re-read it on every collection")
      (is (not= reading-1 reading-2) "retained allocation must change the live reading"))))

(deftest runtime-counters-advance-after-real-work-and-collection
  (let [{:keys [provider meter]} (setup)
        read-point (fn [name]
                     (:value (point-for (metric-named provider name) {})))]
    (runtime/register! meter)
    (let [gc-before (read-point "jolt.gc.count")
          cpu-before (read-point "jolt.cpu.time")
          uptime-before (read-point "process.uptime")
          ;; Retain the result so this is real CPU/allocation work rather than a
          ;; vacuous expression the compiler can discard.
          work (reduce + (range 200000))]
      (jolt.host/gc-full!)
      (is (= 19999900000 work))
      (is (> (read-point "jolt.gc.count") gc-before))
      (is (> (read-point "jolt.cpu.time") cpu-before))
      (is (> (read-point "process.uptime") uptime-before)))))

(deftest unavailable-runtime-primitives-are-omitted-fail-open
  (let [{:keys [provider meter]} (setup)]
    (with-redefs [jolt.host/bytes-allocated
                  (fn [] (throw (ex-info "unavailable" {})))]
      (runtime/register! meter))
    (let [names (->> (sdk/collect! provider)
                     (mapcat :metrics)
                     (map :name)
                     set)]
      (is (not (contains? names "jolt.memory.used")))
      (is (contains? names "jolt.gc.count"))
      (is (contains? names "process.uptime")))))
