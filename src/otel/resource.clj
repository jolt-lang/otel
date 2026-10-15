(ns otel.resource
  "A Resource: the immutable set of attributes identifying the entity that
  produced some telemetry — which service, which process, which host.

  Every span, metric and log the SDK exports carries one, and it is what lets a
  backend answer \"which of my deployments emitted this\". It is assembled once at
  startup from three layers, each overriding the last: the SDK defaults, what the
  environment declares (OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES), and what
  the application passes explicitly."
  (:refer-clojure :exclude [merge])
  (:require [clojure.string :as str]
            [jolt.ffi :as ffi]
            [otel.attributes :as attr]))

(def sdk-version
  "This library's version, reported as telemetry.sdk.version."
  "0.3.0")

(defrecord Resource [attributes schema-url])

(defn resource
  "A resource carrying `attrs`. `:schema-url` names the semantic-convention
  schema the attributes follow."
  ([attrs] (resource attrs {}))
  ([attrs {:keys [schema-url]}]
   (->Resource (attr/normalize attrs) schema-url)))

(def empty-resource (resource {}))

(defn attributes [r] (:attributes r))
(defn schema-url [r] (:schema-url r))

(defn merge-resources
  "Merge resources left to right. On a key conflict the updating (right-hand)
  resource wins, and its schema-url is taken when it has one — the spec's
  ordering, which is what lets application config override detected defaults."
  ([] empty-resource)
  ([r] r)
  ([old new]
   (->Resource (clojure.core/merge (:attributes old) (:attributes new))
               (or (:schema-url new) (:schema-url old))))
  ([old new & more] (reduce merge-resources (merge-resources old new) more)))

;; --- detectors --------------------------------------------------------------

(defn sdk-resource
  "The attributes identifying this SDK. Required on every resource."
  []
  (resource {:telemetry.sdk.name "opentelemetry"
             :telemetry.sdk.language "jolt"
             :telemetry.sdk.version sdk-version}))

;; The process id comes from libc: Chez has no getpid of its own, and jolt's
;; System/getProperty has no JVM behind it to ask. Wrapped at the call site
;; because a host that cannot bind the symbol should cost us the one attribute,
;; not the whole resource.
(ffi/defcfn c-getpid "getpid" [] :int)

(defn- pid []
  (try (c-getpid) (catch :default _ nil)))

(defn- probe
  "Call `f`, yielding nil if it fails. Resource detection is best-effort: a host
  that cannot answer one of these questions — an older jolt without a given
  jolt.host primitive, say — should cost that single attribute, not the whole
  resource and with it the SDK's startup."
  [f]
  (try (f) (catch :default _ nil)))

(defn process-resource
  "Attributes describing the running process and the runtime under it."
  []
  (let [pid (pid)
        scheme (probe #(jolt.host/scheme-version))
        jolt-v (probe #(jolt.host/jolt-version))]
    (resource
      (cond-> {:process.runtime.name "Chez Scheme"}
        scheme (assoc :process.runtime.version scheme)
        ;; The jolt layer is what the application actually runs on, so the
        ;; description names both it and the Scheme underneath.
        (or scheme jolt-v) (assoc :process.runtime.description
                                  (str "jolt " (or jolt-v "?") " on " (or scheme "Chez Scheme")))
        pid (assoc :process.pid pid)))))

(defn- machine->arch
  "The OTel host.arch value for a Chez machine-type tag (a6le, ta6le, tarm64osx,
  arm32le, i3nt, …). The leading `t` marks a threaded build and is not part of
  the architecture."
  [m]
  (let [m (if (str/starts-with? m "t") (subs m 1) m)]
    (cond
      (str/starts-with? m "arm64") "arm64"
      (str/starts-with? m "arm32") "arm32"
      (str/starts-with? m "a6") "amd64"
      (str/starts-with? m "i3") "x86"
      (str/starts-with? m "ppc32") "ppc32"
      (str/starts-with? m "rv64") "riscv64"
      (str/starts-with? m "la64") "loong64"
      :else m)))

(defn- os-type
  "The OTel os.type value, from the host's OS name."
  [os-name]
  (let [n (str/lower-case (or os-name ""))]
    (cond
      (str/includes? n "mac") "darwin"
      (str/includes? n "win") "windows"
      (str/includes? n "linux") "linux"
      (str/includes? n "bsd") "freebsd"
      (str/includes? n "sunos") "solaris"
      :else (if (empty? n) "unknown" n))))

(defn host-resource
  "Attributes describing the machine and operating system."
  []
  (let [machine (probe #(jolt.host/machine-type))]
    (resource (cond-> {:os.type (os-type (System/getProperty "os.name"))}
                machine (assoc :host.arch (machine->arch machine))))))

;; --- environment ------------------------------------------------------------

(defn- percent-decode
  "Decode %XX escapes. OTEL_RESOURCE_ATTRIBUTES uses W3C Baggage syntax, in which
  a value containing a comma, equals or space arrives percent-encoded."
  [s]
  (if-not (str/includes? s "%")
    s
    (let [sb (StringBuilder.)
          n (count s)]
      (loop [i 0]
        (if (>= i n)
          (.toString sb)
          (let [c (.charAt s i)]
            (if (and (= c \%) (<= (+ i 2) (dec n)))
              (let [hex (subs s (inc i) (+ i 3))
                    b (try (Integer/parseInt hex 16) (catch :default _ nil))]
                (if b
                  (do (.append sb (char b)) (recur (+ i 3)))
                  (do (.append sb c) (recur (inc i)))))
              (do (.append sb c) (recur (inc i))))))))))

(defn parse-resource-attributes
  "Parse an OTEL_RESOURCE_ATTRIBUTES value: comma-separated key=value pairs, with
  percent-encoded values. A malformed entry is skipped rather than failing the
  whole list — a bad env var must not stop the process from starting."
  [s]
  (if (str/blank? s)
    {}
    (reduce (fn [acc entry]
              (let [i (str/index-of entry "=")]
                (if (nil? i)
                  acc
                  (let [k (str/trim (subs entry 0 i))
                        v (str/trim (subs entry (inc i)))]
                    (if (str/blank? k)
                      acc
                      (assoc acc k (percent-decode v)))))))
            {}
            (str/split s #","))))

(defn env-resource
  "The resource declared by the environment: OTEL_RESOURCE_ATTRIBUTES, with
  OTEL_SERVICE_NAME taking precedence for service.name as the spec requires."
  []
  (let [attrs (parse-resource-attributes (jolt.host/getenv "OTEL_RESOURCE_ATTRIBUTES"))
        svc (jolt.host/getenv "OTEL_SERVICE_NAME")]
    (resource (cond-> attrs
                (not (str/blank? svc)) (assoc "service.name" svc)))))

(def default-service-name
  "The spec's fallback when nothing names the service: unknown_service:<runtime>."
  "unknown_service:jolt")

(defn default-resource
  "The resource the SDK uses when the application supplies none: SDK identity,
  process and host detection, environment configuration, and a service.name
  fallback if nothing above provided one."
  []
  (merge-resources (resource {:service.name default-service-name})
                   (sdk-resource)
                   (process-resource)
                   (host-resource)
                   (env-resource)))
