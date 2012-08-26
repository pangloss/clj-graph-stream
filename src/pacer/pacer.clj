(ns pacer
    (:import
      (com.tinkerpop.blueprints Graph Element Vertex Edge TransactionalGraph IndexableGraph Index)
      (com.tinkerpop.blueprints.impls.tg TinkerGraph))
    )

(defn simple-encoder [] { :encode nil :decode nil })
(defn tg []
  { :type :graph
    :raw-graph (com.tinkerpop.blueprints.impls.tg.TinkerGraph.)
    :encoder (simple-encoder)})

(defn v
  ([]
   [{ :source-type :graph
      :type :vertex
      :name "GraphV"
      :iterator (fn [source] (doto (:graph source) (.getVertices) (.iterator))) }])
  ([graph]
   (concat [graph] (v))))

(defn- pipe-from-step [source step]
       (prn step)
       (cond
         (:pipe step) (doto (:pipe step)
                            (.setStarts source))
         (:build-pipe step) (throw (Exception. "Not Implemented"))
         (:iterator step) ((:iterator step) source)
         :else (throw (Exception. "Don't know how to build step"))))

(defn pipe
  ([[source step & route]]
   (if route
     (pipe (pipe-from-step source step) route)
     (pipe-from-step source step)))
  ([iter [step & route]]
   (if route
     (recur (pipe-from-step iter step) route)
     (pipe-from-step iter step))))
