(require '[otel.instrument.runtime :as runtime]
         '[otel.resource :as resource]
         '[otel.sdk.clock :as clock]
         '[otel.sdk.metrics :as metrics])

(def iterations 20000)

(defn provider [registered?]
  (let [p (metrics/meter-provider
            {:resource resource/empty-resource
             :clock (clock/fake-clock {:wall 1000 :mono 0})})]
    (when registered?
      (runtime/register! (metrics/get-meter p {:name "bench"})))
    p))

(defn run-once [p n]
  (jolt.host/gc-full!)
  (let [gc-before (jolt.host/gc-bytes)
        start (System/nanoTime)]
    (dotimes [_ n]
      (metrics/collect! p))
    {:ns-per-collection (/ (double (- (System/nanoTime) start)) n)
     :gc-bytes (- (jolt.host/gc-bytes) gc-before)}))

(defn median [xs]
  (nth (sort xs) (quot (count xs) 2)))

(defn measure [registered?]
  (let [p (provider registered?)]
    (run-once p 1000)
    (let [runs (vec (repeatedly 7 #(run-once p iterations)))]
      {:runs runs
       :median-ns-per-collection (median (map :ns-per-collection runs))
       :median-gc-bytes (median (map :gc-bytes runs))})))

(prn {:jolt (jolt.host/jolt-version)
      :chez (jolt.host/scheme-version)
      :iterations iterations
      :unregistered (measure false)
      :runtime-registered (measure true)})
