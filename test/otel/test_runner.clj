(ns otel.test-runner
  "Runs the library's test suite: `jolt test`, or

      jolt -M:test -m otel.test-runner [ns-or-prefix ...]

  With no arguments every namespace in `all-namespaces` runs. An argument selects
  the namespaces whose name contains it, so `... otel.trace` runs the trace
  suites alone."
  (:require [clojure.test :as t]
            [clojure.string :as str]))

;; Listed explicitly rather than discovered by scanning the test directory: jolt
;; resolves requires off the source roots, and an explicit list is also what the
;; AOT closure needs to see.
(def all-namespaces
  '[otel.attributes-test
    otel.context-test
    otel.sdk.clock-test
    otel.sdk.exemplar-test
    otel.sdk-test
    otel.sdk.logs-test
    otel.sdk.metrics-test
    otel.sdk.sampler-test
    otel.sdk.tracer-test
    otel.id-test
    otel.exporter.otlp-test
    otel.otlp.encode-test
    otel.propagation-test
    otel.resource-test
    otel.trace-test])

(defn -main [& args]
  (let [selected (if (seq args)
                   (filter (fn [ns-sym]
                             (some #(str/includes? (str ns-sym) %) args))
                           all-namespaces)
                   all-namespaces)]
    (when (empty? selected)
      (println "no test namespaces matched" (pr-str args))
      (System/exit 1))
    (doseq [n selected] (require n))
    (let [{:keys [fail error] :as summary} (apply t/run-tests selected)]
      (println)
      (println (format "%d failures, %d errors" fail error))
      (System/exit (if (pos? (+ fail error)) 1 0)))))
