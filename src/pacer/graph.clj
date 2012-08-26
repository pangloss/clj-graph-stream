(ns pacer.graph
    (:import
      (com.tinkerpop.blueprints.impls.tg TinkerGraph)
      (com.tinkerpop.gremlin.pipes.transform InEdgesPipe OutEdgesPipe))
    (:use [clojure.pprint :only [pprint]]
          pacer.step))

(defn tg []
  { :source true
    :type :graph
    :name "TinkerGraph"
    :raw-graph (atom (com.tinkerpop.blueprints.impls.tg.TinkerGraph.))
    :encoder (atom (pacer/simple-encoder))})

(defn create-vertex [graph]
  { :graph graph
    :type :vertex
    :element (.addVertex @(:raw-graph graph) nil)
    })

(defn create-edge [graph label from to]
  { :graph graph
    :type :edge
    :element (.addEdge @(:raw-graph graph) nil (:element from) (:element to) (str label))
    })

(defn v []
  { :source-type :graph
    :type :vertex
    :name "V"
    :iterator (fn iterator [source]
                  (.. @(:raw-graph source) getVertices iterator)) })

(defn e []
  { :source-type :graph
    :type :edge
    :name "E"
    :iterator (fn iterator [source]
                  (.. @(:raw-graph source) getEdges iterator)) })

(defn out-e [& labels]
  { :source-type :vertex
     :type :edge
     :name "OutE"
     :pipe (fn pipe [in]
               (OutEdgesPipe. (into-array String (map str labels)))) })

(defn in-e [& labels]
  { :source-type :vertex
     :type :edge
     :name "InE"
     :pipe (fn pipe [in]
               (InEdgesPipe. (into-array String (map str labels)))) })

