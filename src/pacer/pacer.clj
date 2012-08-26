(ns pacer
    (:import
      (com.tinkerpop.blueprints.impls.tg TinkerGraph))
    (:use [clojure.pprint :only [pprint]]))

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
      :iterator (fn v [source] (.. (:raw-graph source) getVertices iterator)) }])
  ([graph]
   (conj [graph] (first (v)))))

(defn- check-step [in step]
       (when (not= (:source-type step) (:type in))
         (throw (Exception. (str (describe-step step) " expects type "
                                 (:source-type step) " but got " (:type in))))))

(defn- pipe-from-step [in step]
       (cond
         (:pipe step) (doto (:pipe step)
                            (.setStarts (:pipe in (:source in))))
         (:iterator step) ((:iterator step) (:pipe in (:source in)))
         :else (throw (Exception. "Don't know how to build step"))))

(defn pipe
  "Build a pipe from a route definition"
  [[source & route]]
  (reduce (fn [in step]
              (check-step in step)
              { :pipe (pipe-from-step in step)
                :type (:type step (:type in))
                :route (conj (:route in) step)})
          { :source source, :type (:type source), :route [] }
          route))
