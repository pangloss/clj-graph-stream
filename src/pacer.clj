(ns pacer
    (:import
      (com.tinkerpop.blueprints.impls.tg TinkerGraph)
      (com.tinkerpop.gremlin.pipes.transform InEdgesPipe))
    (:use [clojure.pprint :only [pprint]]
          pacer.step))

(defn simple-encoder [] { :encode nil :decode nil })

(defn show
  "Show an easy to read"
  [route]
  (->> route
       (map describe-step)
       (clojure.string/join " -> ")))

(defn- pipe-from-step [in step]
       (cond
         (:pipe step) (doto ((:pipe step) in)
                            (.setStarts (:pipe in (:source in))))
         (:iterator step) ((:iterator step) (:pipe in (:source in)))
         :else (throw (Exception. "Don't know how to build step"))))

(defn build-pipe [[source & route]]
  (if route
    (reduce (fn [in step]
                (check-step in step)
                { :pipe (pipe-from-step in step)
                  :type (:type step (:type in))
                  :route (conj (:route in) step)})
            { :source source, :type (:type source), :route [] }
            route)
    source))

(defn pipe
  "Build a pipe from a route definition"
  [route]
  (:pipe (build-pipe route)))

(defn route [& steps]
  (vec steps))

(defmacro route [& steps]
  (vec (map #(if (list? %) % (list %)) steps)))
