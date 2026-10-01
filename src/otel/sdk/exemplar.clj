(ns otel.sdk.exemplar
  "Metric exemplar eligibility and bounded per-timeseries reservoirs.

  A filter decides whether a measurement may be sampled. A reservoir then
  decides whether to retain it. Reservoir state is reset after every collection
  cycle even for cumulative metric streams, so exported exemplars always come
  from the most recent interval rather than accumulating forever."
  (:require [otel.sdk.clock :as clock]
            [otel.trace :as trace]))

(def filters
  "The built-in OpenTelemetry exemplar filters."
  #{:always-on :always-off :trace-based})

(defn check-filter
  "Return a supported filter or throw before instruments are created."
  [filter]
  (if (contains? filters filter)
    filter
    (throw (ex-info (str "unsupported exemplar filter " (pr-str filter))
                    {:filter filter :supported filters}))))

(defn eligible-context
  "Capture the active span context when `filter` admits this measurement.

  An invalid SpanContext is still a truthy AlwaysOn result, distinguishing an
  eligible measurement outside a span from a rejected measurement (nil)."
  [filter]
  (case filter
    :always-off nil
    :always-on (trace/current-span-context)
    :trace-based (let [span (trace/current-span)]
                   ;; The root context returns the shared invalid span. Avoid a
                   ;; protocol dispatch through it on every untraced metric—the
                   ;; default filter's common path—while retaining sampled
                   ;; non-recording contexts when one is explicitly active.
                   (when-not (identical? span trace/invalid-span)
                     (let [sc (trace/span-context-of span)]
                       (when (and (trace/valid? sc) (trace/sampled? sc))
                         sc))))))

(defn measurement
  "Build the canonical exemplar captured at measurement time."
  [span-context value attributes point-attributes timestamp]
  (let [filtered (reduce-kv (fn [m k v]
                              (if (contains? point-attributes k) m (assoc m k v)))
                            {}
                            attributes)]
    (cond-> {:value value :time-unix-nano timestamp}
      (seq filtered) (assoc :filtered-attributes filtered)
      (trace/valid? span-context) (assoc :trace-id (:trace-id span-context)
                                         :span-id (:span-id span-context)))))

(defn reservoir
  "Create an empty reservoir. Histograms receive one slot per explicit bucket;
  other aggregations receive `size` uniformly sampled slots."
  [kind boundaries size]
  (if (= :histogram kind)
    (let [n (inc (count boundaries))]
      {:kind :aligned-histogram
       :boundaries boundaries
       :seen (vec (repeat n 0))
       :slots (vec (repeat n nil))})
    {:kind :fixed-size
     :seen 0
     :slots (vec (repeat size nil))}))

(defn- bucket-index [boundaries value]
  (loop [i 0]
    (cond
      (>= i (count boundaries)) i
      (<= value (nth boundaries i)) i
      :else (recur (inc i)))))

(defn- replace-slot [slots i exemplar]
  (assoc slots i exemplar))

(defn offer
  "Offer one measurement using `random-int-fn`, which receives an exclusive
  upper bound. The canonical exemplar map is allocated only if the reservoir
  accepts the measurement. Fixed reservoirs use Algorithm R. Histogram
  reservoirs independently sample each explicit bucket."
  [reservoir value eligible attributes point-attributes measurement-clock random-int-fn]
  (case (:kind reservoir)
    :fixed-size
    (let [seen (:seen reservoir)
          size (count (:slots reservoir))
          candidate (if (< seen size) seen (random-int-fn (inc seen)))]
      (cond-> (update reservoir :seen inc)
        (< candidate size)
        (update :slots replace-slot candidate
                (measurement eligible value attributes point-attributes
                             (clock/wall-nanos measurement-clock)))))

    :aligned-histogram
    (let [bucket (bucket-index (:boundaries reservoir) value)
          seen (nth (:seen reservoir) bucket)
          candidate (random-int-fn (inc seen))]
      (cond-> (update-in reservoir [:seen bucket] inc)
        (zero? candidate)
        (update :slots replace-slot bucket
                (measurement eligible value attributes point-attributes
                             (clock/wall-nanos measurement-clock)))))))

(defn collect
  "Return retained exemplars in reservoir-slot order."
  [reservoir]
  (into [] (remove nil?) (:slots reservoir)))

(defn reset
  "Clear samples and sampling counts for the next collection cycle."
  [reservoir]
  (case (:kind reservoir)
    :fixed-size (assoc reservoir
                       :seen 0
                       :slots (vec (repeat (count (:slots reservoir)) nil)))
    :aligned-histogram (assoc reservoir
                              :seen (vec (repeat (count (:seen reservoir)) 0))
                              :slots (vec (repeat (count (:slots reservoir)) nil)))))
