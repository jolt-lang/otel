(ns otel.sdk.tracer-test
  (:require [clojure.test :refer [deftest is testing]]
            [otel.context :as ctx]
            [otel.exporter.memory :as memory]
            [otel.id :as id]
            [otel.resource :as res]
            [otel.sdk.clock :as clock]
            [otel.sdk.export :as export]
            [otel.sdk.sampler :as sampler]
            [otel.sdk.tracer :as sdk]
            [otel.trace :as trace]))

(defn- setup
  "A provider wired to an in-memory exporter through a simple processor, plus a
  controllable clock. Returns {:provider :tracer :exporter :clock}."
  ([] (setup {}))
  ([opts]
   (let [exporter (memory/exporter)
         fake (clock/fake-clock {:wall 1000000000 :mono 0})
         provider (sdk/tracer-provider
                    (merge {:resource (res/resource {:service.name "test-svc"})
                            :processors [(export/simple-processor exporter)]
                            :clock fake}
                           opts))]
     {:provider provider
      :tracer (sdk/get-tracer provider {:name "test-scope" :version "1.2.3"})
      :exporter exporter
      :clock fake})))

;; --- basic recording --------------------------------------------------------

(deftest a-finished-span-reaches-the-exporter
  (let [{:keys [tracer exporter]} (setup)]
    (trace/with-span [sp tracer "checkout"]
      (is (trace/recording? sp)))
    (let [[s :as all] (memory/spans exporter)]
      (is (= 1 (count all)))
      (is (= "checkout" (:name s))))))

(deftest span-carries-a-valid-context
  (let [{:keys [tracer exporter]} (setup)]
    (trace/with-span [sp tracer "op"])
    (let [s (first (memory/spans exporter))]
      (is (id/valid-trace-id? (get-in s [:span-context :trace-id])))
      (is (id/valid-span-id? (get-in s [:span-context :span-id]))))))

(deftest span-defaults
  (let [{:keys [tracer exporter]} (setup)]
    (trace/with-span [sp tracer "op"])
    (let [s (first (memory/spans exporter))]
      (is (= :internal (:kind s)))
      (is (= :unset (get-in s [:status :code])))
      (is (= {} (:attributes s)))
      (is (= [] (:events s)))
      (is (= [] (:links s)))
      (is (nil? (:parent-span-id s))))))

(deftest span-records-the-scope-and-resource
  (let [{:keys [tracer exporter]} (setup)]
    (trace/with-span [sp tracer "op"])
    (let [s (first (memory/spans exporter))]
      (is (= "test-scope" (get-in s [:scope :name])))
      (is (= "1.2.3" (get-in s [:scope :version])))
      (is (= "test-svc" (get (res/attributes (:resource s)) "service.name"))))))

;; --- timing -----------------------------------------------------------------

(deftest timestamps-come-from-the-clock
  (let [{:keys [tracer exporter clock]} (setup)
        sp (trace/start-span tracer "op")]
    (clock/advance! clock {:mono 500 :wall 500})
    (trace/end! sp)
    (let [s (first (memory/spans exporter))]
      (is (= 1000000000 (:start-time-unix-nano s)))
      (is (= 1000000500 (:end-time-unix-nano s))))))

(deftest duration-is-immune-to-a-wall-clock-step
  (testing "the provider anchors its clock, so an ntp step during a span cannot
            produce a negative duration"
    (let [{:keys [tracer exporter clock]} (setup)
          sp (trace/start-span tracer "op")]
      (clock/advance! clock {:mono 500})
      (clock/set-wall! clock 1)               ; wall clock jumps backwards
      (trace/end! sp)
      (let [s (first (memory/spans exporter))]
        (is (= 500 (- (:end-time-unix-nano s) (:start-time-unix-nano s))))))))

(deftest explicit-timestamps-are-honoured
  (let [{:keys [tracer exporter]} (setup)
        sp (trace/start-span tracer "op" {:start-timestamp 42})]
    (trace/end! sp 99)
    (let [s (first (memory/spans exporter))]
      (is (= 42 (:start-time-unix-nano s)))
      (is (= 99 (:end-time-unix-nano s))))))

;; --- attributes, events, links, status --------------------------------------

(deftest attributes-are-recorded-and-normalized
  (let [{:keys [tracer exporter]} (setup)]
    (trace/with-span [sp tracer "op"]
      (trace/set-attribute! sp :http.method "GET")
      (trace/set-attributes! sp {:http.status 200 :ok true :dropped nil}))
    (let [a (:attributes (first (memory/spans exporter)))]
      (is (= "GET" (get a "http.method")))
      (is (= 200 (get a "http.status")))
      (is (= true (get a "ok")))
      (is (not (contains? a "dropped"))))))

(deftest start-attributes-are-recorded
  (let [{:keys [tracer exporter]} (setup)]
    (trace/with-span [sp tracer "op" {:attributes {:a 1}}])
    (is (= 1 (get (:attributes (first (memory/spans exporter))) "a")))))

(deftest events-are-recorded-in-order-with-timestamps
  (let [{:keys [tracer exporter clock]} (setup)]
    (let [sp (trace/start-span tracer "op")]
      (trace/add-event! sp "first")
      (clock/advance! clock {:mono 10})
      (trace/add-event! sp "second" {:k 1})
      (trace/end! sp))
    (let [[e1 e2] (:events (first (memory/spans exporter)))]
      (is (= "first" (:name e1)))
      (is (= 1000000000 (:timestamp-unix-nano e1)))
      (is (= "second" (:name e2)))
      (is (= 1000000010 (:timestamp-unix-nano e2)))
      (is (= 1 (get (:attributes e2) "k"))))))

(deftest links-are-recorded
  (let [{:keys [tracer exporter]} (setup)
        linked (trace/span-context {:trace-id (id/trace-id) :span-id (id/span-id) :sampled? true})]
    (trace/with-span [sp tracer "op" {:links [{:span-context linked :attributes {:rel "follows"}}]}])
    (let [[l] (:links (first (memory/spans exporter)))]
      (is (= (:trace-id linked) (get-in l [:span-context :trace-id])))
      (is (= "follows" (get (:attributes l) "rel"))))))

(deftest status-can-be-set
  (let [{:keys [tracer exporter]} (setup)]
    (trace/with-span [sp tracer "op"]
      (trace/set-status! sp :error "it broke"))
    (let [st (:status (first (memory/spans exporter)))]
      (is (= :error (:code st)))
      (is (= "it broke" (:description st))))))

(deftest ok-status-is-final
  (testing "spec: once a span is explicitly marked :ok, nothing may downgrade it"
    (let [{:keys [tracer exporter]} (setup)]
      (trace/with-span [sp tracer "op"]
        (trace/set-status! sp :ok)
        (trace/set-status! sp :error "too late"))
      (is (= :ok (get-in (first (memory/spans exporter)) [:status :code]))))))

(deftest unset-never-overrides
  (let [{:keys [tracer exporter]} (setup)]
    (trace/with-span [sp tracer "op"]
      (trace/set-status! sp :error "bad")
      (trace/set-status! sp :unset))
    (is (= :error (get-in (first (memory/spans exporter)) [:status :code])))))

(deftest error-status-drops-the-description-for-non-error-codes
  (testing "spec: description is only meaningful on :error"
    (let [{:keys [tracer exporter]} (setup)]
      (trace/with-span [sp tracer "op"]
        (trace/set-status! sp :ok "ignored"))
      (is (nil? (get-in (first (memory/spans exporter)) [:status :description]))))))

(deftest name-can-be-updated
  (let [{:keys [tracer exporter]} (setup)]
    (trace/with-span [sp tracer "op"]
      (trace/update-name! sp "GET /users/:id"))
    (is (= "GET /users/:id" (:name (first (memory/spans exporter)))))))

;; --- exceptions -------------------------------------------------------------

(deftest with-span-records-a-thrown-exception
  (let [{:keys [tracer exporter]} (setup)]
    (is (thrown? Exception
                 (trace/with-span [sp tracer "op"]
                   (throw (ex-info "boom" {:a 1})))))
    (let [s (first (memory/spans exporter))
          [e] (:events s)]
      (is (= :error (get-in s [:status :code])))
      (is (= "boom" (get-in s [:status :description])))
      (is (= "exception" (:name e)))
      (is (= "boom" (get (:attributes e) "exception.message")))
      (is (string? (get (:attributes e) "exception.type"))))))

(deftest span-is-ended-even-when-the-body-throws
  (let [{:keys [tracer exporter]} (setup)]
    (is (thrown? Exception (trace/with-span [sp tracer "op"] (throw (ex-info "x" {})))))
    (is (= 1 (count (memory/spans exporter))))))

(deftest record-exception-does-not-set-status
  (testing "spec: recording an exception is not by itself a failure"
    (let [{:keys [tracer exporter]} (setup)]
      (trace/with-span [sp tracer "op"]
        (trace/record-exception! sp (ex-info "handled" {})))
      (is (= :unset (get-in (first (memory/spans exporter)) [:status :code]))))))

;; --- parenting --------------------------------------------------------------

(deftest a-nested-span-takes-the-active-span-as-parent
  (let [{:keys [tracer exporter]} (setup)]
    (trace/with-span [outer tracer "outer"]
      (trace/with-span [inner tracer "inner"]))
    (let [spans (memory/spans exporter)
          inner (first (filter #(= "inner" (:name %)) spans))
          outer (first (filter #(= "outer" (:name %)) spans))]
      (is (= (get-in outer [:span-context :trace-id]) (get-in inner [:span-context :trace-id]))
          "a child stays in its parent's trace")
      (is (= (get-in outer [:span-context :span-id]) (:parent-span-id inner)))
      (is (nil? (:parent-span-id outer))))))

(deftest an-explicit-parent-context-overrides-the-active-span
  (let [{:keys [tracer exporter]} (setup)
        remote (trace/span-context {:trace-id (id/trace-id) :span-id (id/span-id)
                                    :sampled? true :remote? true})
        parent-ctx (trace/context-with-span ctx/root (trace/non-recording-span remote))]
    (trace/with-span [outer tracer "outer"]
      (trace/with-span [child tracer "child" {:parent parent-ctx}]))
    (let [child (first (filter #(= "child" (:name %)) (memory/spans exporter)))]
      (is (= (:trace-id remote) (get-in child [:span-context :trace-id])))
      (is (= (:span-id remote) (:parent-span-id child))))))

(deftest a-root-can-be-forced
  (let [{:keys [tracer exporter]} (setup)]
    (trace/with-span [outer tracer "outer"]
      (trace/with-span [child tracer "child" {:parent ctx/root}]))
    (let [spans (memory/spans exporter)
          child (first (filter #(= "child" (:name %)) spans))
          outer (first (filter #(= "outer" (:name %)) spans))]
      (is (nil? (:parent-span-id child)))
      (is (not= (get-in outer [:span-context :trace-id])
                (get-in child [:span-context :trace-id]))))))

(deftest the-span-is-current-inside-its-body
  (let [{:keys [tracer]} (setup)]
    (trace/with-span [sp tracer "op"]
      (is (= (trace/span-context-of sp) (trace/current-span-context))))
    (is (not (trace/valid? (trace/current-span-context))))))

;; --- sampling ---------------------------------------------------------------

(deftest a-dropped-span-is-not-exported
  (let [{:keys [tracer exporter]} (setup {:sampler sampler/always-off})]
    (trace/with-span [sp tracer "op"]
      (is (not (trace/recording? sp)))
      (is (not (trace/sampled? (trace/span-context-of sp)))))
    (is (= [] (memory/spans exporter)))))

(deftest a-dropped-span-still-propagates-its-context
  (testing "a sampled-out span must still carry a valid trace id, so downstream
            services join the same (unsampled) trace rather than starting a new one"
    (let [{:keys [tracer]} (setup {:sampler sampler/always-off})]
      (trace/with-span [sp tracer "op"]
        (is (trace/valid? (trace/span-context-of sp)))))))

(deftest a-child-of-a-dropped-span-keeps-the-trace-id
  (let [{:keys [tracer]} (setup {:sampler sampler/always-off})]
    (trace/with-span [outer tracer "outer"]
      (trace/with-span [inner tracer "inner"]
        (is (= (get-in (trace/span-context-of outer) [:trace-id])
               (get-in (trace/span-context-of inner) [:trace-id])))))))

(deftest sampled-spans-carry-the-sampled-flag
  (let [{:keys [tracer exporter]} (setup {:sampler sampler/always-on})]
    (trace/with-span [sp tracer "op"]
      (is (trace/sampled? (trace/span-context-of sp))))
    (is (= (bit-or trace/flag-sampled trace/flag-random)
           (get-in (first (memory/spans exporter)) [:span-context :trace-flags])))))

(deftest children-preserve-the-parents-random-trace-id-flag
  (doseq [[parent-flags sampler expected]
          [[(bit-or trace/flag-sampled trace/flag-random)
            sampler/always-off
            trace/flag-random]
           [trace/flag-random
            sampler/always-on
            (bit-or trace/flag-sampled trace/flag-random)]]]
    (let [{:keys [tracer]} (setup {:sampler sampler})
          parent (trace/span-context {:trace-id (id/trace-id)
                                      :span-id (id/span-id)
                                      :trace-flags parent-flags
                                      :remote? true})
          parent-context (trace/context-with-span
                           ctx/root (trace/non-recording-span parent))
          child (trace/start-span tracer "child" {:parent parent-context})]
      (is (= expected (:trace-flags (trace/span-context-of child)))))))

;; --- ended span semantics ---------------------------------------------------

(deftest mutations-after-end-are-ignored
  (let [{:keys [tracer exporter]} (setup)
        sp (trace/start-span tracer "op")]
    (trace/end! sp)
    (trace/set-attribute! sp :late 1)
    (trace/add-event! sp "late")
    (trace/set-status! sp :error "late")
    (let [s (first (memory/spans exporter))]
      (is (= {} (:attributes s)))
      (is (= [] (:events s)))
      (is (= :unset (get-in s [:status :code]))))))

(deftest ending-twice-exports-once
  (let [{:keys [tracer exporter]} (setup)
        sp (trace/start-span tracer "op")]
    (trace/end! sp)
    (trace/end! sp)
    (is (= 1 (count (memory/spans exporter))))))

(deftest an-ended-span-is-not-recording
  (let [{:keys [tracer]} (setup)
        sp (trace/start-span tracer "op")]
    (is (trace/recording? sp))
    (trace/end! sp)
    (is (not (trace/recording? sp)))))

;; --- limits -----------------------------------------------------------------

(deftest attribute-count-limit-is-enforced-and-counted
  (let [{:keys [tracer exporter]} (setup {:limits {:attribute-count-limit 2}})]
    (trace/with-span [sp tracer "op"]
      (trace/set-attributes! sp {:a 1 :b 2 :c 3 :d 4}))
    (let [s (first (memory/spans exporter))]
      (is (= 2 (count (:attributes s))))
      (is (= 2 (:dropped-attributes-count s))))))

(deftest event-count-limit-is-enforced-and-counted
  (let [{:keys [tracer exporter]} (setup {:limits {:event-count-limit 2}})]
    (trace/with-span [sp tracer "op"]
      (dotimes [i 5] (trace/add-event! sp (str "e" i))))
    (let [s (first (memory/spans exporter))]
      (is (= 2 (count (:events s))))
      (is (= 3 (:dropped-events-count s))))))

(deftest link-count-limit-is-enforced-and-counted
  (let [{:keys [tracer exporter]} (setup {:limits {:link-count-limit 1}})
        link (fn [] {:span-context (trace/span-context {:trace-id (id/trace-id)
                                                        :span-id (id/span-id)})})]
    (trace/with-span [sp tracer "op" {:links [(link) (link) (link)]}])
    (let [s (first (memory/spans exporter))]
      (is (= 1 (count (:links s))))
      (is (= 2 (:dropped-links-count s))))))

;; --- processors -------------------------------------------------------------

(deftest on-start-is-called-with-the-parent-context
  (let [started (atom [])
        proc (reify export/SpanProcessor
               (on-start [_ span _] (swap! started conj (:name @(:state span))) nil)
               (on-end [_ _] nil)
               (force-flush! [_] true)
               (shutdown! [_] true))
        provider (sdk/tracer-provider {:processors [proc] :resource res/empty-resource})
        tracer (sdk/get-tracer provider {:name "s"})]
    (trace/with-span [sp tracer "op"])
    (is (= ["op"] @started))))

(deftest multiple-processors-all-receive-the-span
  (let [e1 (memory/exporter)
        e2 (memory/exporter)
        provider (sdk/tracer-provider {:processors [(export/simple-processor e1)
                                                    (export/simple-processor e2)]
                                       :resource res/empty-resource})
        tracer (sdk/get-tracer provider {:name "s"})]
    (trace/with-span [sp tracer "op"])
    (is (= 1 (count (memory/spans e1))))
    (is (= 1 (count (memory/spans e2))))))

(deftest provider-shutdown-flushes-and-stops
  (let [exporter (memory/exporter)
        provider (sdk/tracer-provider {:processors [(export/simple-processor exporter)]
                                       :resource res/empty-resource})
        tracer (sdk/get-tracer provider {:name "s"})]
    (trace/with-span [sp tracer "op"])
    (is (sdk/shutdown! provider))
    (is (memory/shutdown? exporter))
    (testing "spans started after shutdown are not recording"
      (trace/with-span [sp tracer "after"]
        (is (not (trace/recording? sp))))
      (is (= 1 (count (memory/spans exporter)))))))

;; --- batch processor --------------------------------------------------------

(deftest batch-processor-exports-on-force-flush
  (let [exporter (memory/exporter)
        proc (export/batch-processor exporter {:schedule-delay-ms 60000})
        provider (sdk/tracer-provider {:processors [proc] :resource res/empty-resource})
        tracer (sdk/get-tracer provider {:name "s"})]
    (try
      (dotimes [i 3] (trace/with-span [sp tracer (str "op" i)]))
      (testing "nothing is exported before the flush — that is the point of batching"
        (is (= [] (memory/spans exporter))))
      (export/force-flush! proc)
      (is (= 3 (count (memory/spans exporter))))
      (finally (export/shutdown! proc)))))

(deftest batch-processor-exports-in-batches
  (let [exporter (memory/exporter)
        proc (export/batch-processor exporter {:schedule-delay-ms 60000
                                               :max-export-batch-size 2})
        provider (sdk/tracer-provider {:processors [proc] :resource res/empty-resource})
        tracer (sdk/get-tracer provider {:name "s"})]
    (try
      (dotimes [i 5] (trace/with-span [sp tracer (str "op" i)]))
      (export/force-flush! proc)
      (is (= 5 (count (memory/spans exporter))))
      (testing "delivered as separate export calls of at most the batch size"
        (is (= [2 2 1] (map count (memory/batches exporter)))))
      (finally (export/shutdown! proc)))))

(deftest batch-processor-drops-when-the-queue-is-full
  (let [exporter (memory/exporter)
        proc (export/batch-processor exporter {:schedule-delay-ms 60000 :max-queue-size 2})
        provider (sdk/tracer-provider {:processors [proc] :resource res/empty-resource})
        tracer (sdk/get-tracer provider {:name "s"})]
    (try
      (dotimes [i 10] (trace/with-span [sp tracer (str "op" i)]))
      (export/force-flush! proc)
      (testing "a full queue drops spans rather than blocking the application"
        (is (= 2 (count (memory/spans exporter))))
        (is (= 8 (export/dropped-count proc))))
      (finally (export/shutdown! proc)))))

(deftest batch-processor-shutdown-flushes
  (let [exporter (memory/exporter)
        proc (export/batch-processor exporter {:schedule-delay-ms 60000})
        provider (sdk/tracer-provider {:processors [proc] :resource res/empty-resource})
        tracer (sdk/get-tracer provider {:name "s"})]
    (dotimes [i 3] (trace/with-span [sp tracer (str "op" i)]))
    (export/shutdown! proc)
    (is (= 3 (count (memory/spans exporter))))
    (is (memory/shutdown? exporter))))

(deftest batch-processor-exports-on-its-own-schedule
  (let [exporter (memory/exporter)
        proc (export/batch-processor exporter {:schedule-delay-ms 20})
        provider (sdk/tracer-provider {:processors [proc] :resource res/empty-resource})
        tracer (sdk/get-tracer provider {:name "s"})]
    (try
      (trace/with-span [sp tracer "op"])
      ;; The worker thread must export without anyone asking it to.
      (loop [waited 0]
        (when (and (empty? (memory/spans exporter)) (< waited 3000))
          (Thread/sleep 20)
          (recur (+ waited 20))))
      (is (= 1 (count (memory/spans exporter))))
      (finally (export/shutdown! proc)))))

(deftest batch-processor-only-queues-sampled-spans
  (let [exporter (memory/exporter)
        proc (export/batch-processor exporter {:schedule-delay-ms 60000})
        provider (sdk/tracer-provider {:processors [proc] :resource res/empty-resource
                                       :sampler sampler/always-off})
        tracer (sdk/get-tracer provider {:name "s"})]
    (try
      (dotimes [i 3] (trace/with-span [sp tracer (str "op" i)]))
      (export/force-flush! proc)
      (is (= [] (memory/spans exporter)))
      (finally (export/shutdown! proc)))))
