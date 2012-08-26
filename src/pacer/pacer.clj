(ns pacer
    (:import
      (com.tinkerpop.blueprints.impls.tg TinkerGraph)))

(defn simple-encoder [] { :encode nil :decode nil })
(defn tg []
  { :type :graph
    :name "TinkerGraph"
    :raw-graph (com.tinkerpop.blueprints.impls.tg.TinkerGraph.)
    :encoder (simple-encoder)})

(defn describe-step [step]
  (:name step))

(defn show
  "Show an easy to read"
  [route]
  (->> route
       (map describe-step)
       (clojure.string/join " -> ")))
(defn v
  ([]
   [{ :source-type :graph
      :type :vertex
      :name "GraphV"
      :iterator (fn [source] (.. (:raw-graph source) getVertices iterator)) }])
  ([graph]
   (conj [graph] (first (v)))))

(defn- pipe-from-step [source step]
       (cond
         (:pipe step) (doto (:pipe step)
                            (.setStarts source))
         (:build-pipe step) (throw (Exception. "Not Implemented"))
         (:iterator step) ((:iterator step) source)
         :else (throw (Exception. "Don't know how to build step"))))

(defn pipe
  "Build a pipe from a route definition"
  ([[source step & route]]
   (if route
     (pipe (pipe-from-step source step) route)
     (pipe-from-step source step)))
  ([iter [step & route]]
   (if route
     (recur (pipe-from-step iter step) route)
     (pipe-from-step iter step))))

