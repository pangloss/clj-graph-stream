(ns pacer.graph
    (:import
      (com.tinkerpop.blueprints.impls.tg TinkerGraph)
      (com.tinkerpop.gremlin.pipes.transform InEdgesPipe OutEdgesPipe BothEdgesPipe
                                             InPipe OutPipe BothPipe
                                             InVertexPipe OutVertexPipe BothVerticesPipe
                                             PropertyMapPipe LabelPipe
                                             ))
    (:use [clojure.pprint :only [pprint]]
          [clojure.string :only [join]]
          pacer.step))

(defprotocol PacerGraph
             (create-vertex [graph])
             (create-edge [graph label from to])
             (encode [graph value])
             (decode [graph value]))

(defrecord Vertex [graph element])
(defrecord Edge [graph element])

(defrecord Graph [name raw-graph encoder]
           Object
           (toString [g] (str @(:raw-graph g)))
           PacerGraph
           (create-vertex [graph]
                          (Vertex. graph (.addVertex @(:raw-graph graph) nil)))
           (create-edge [graph label from to]
                        (Edge. graph (.addEdge @(:raw-graph graph) nil (:element from) (:element to) (str label))))
           (encode [graph value] ((:encode encoder) value))
           (decode [graph value] ((:decode encoder) value)))

(defn tg []
  (Graph. "TinkerGraph"
          (atom (com.tinkerpop.blueprints.impls.tg.TinkerGraph.))
          (atom (pacer/simple-encoder))))


(step-type GraphSourced [source-type type name iterator]
           IteratorStep
           (iterator [s in] (iterator s in)))

(defn v []
  (GraphSourced. :graph :vertex "V"
              (fn iterator [source]
                  (.. @(:raw-graph source) getVertices iterator))))

(defn e []
  (GraphSourced. :graph :edge "E"
              (fn iterator [source]
                  (.. @(:raw-graph source) getEdges iterator))))

(defn- name+ [name labels]
       (if (empty? labels)
         name
         (str name " (" (join ", " labels) ")")))

(defn- strs [args]
       (into-array String (map str args)))

(step-type EdgeStep [source-type type name labels pipe-fn]
           PipeStep
           (build-pipe [step in] (pipe-fn step in)))

(defmacro edge-step [type name labels pipe]
  `(EdgeStep. :vertex ~type (name+ ~name ~labels) ~labels
              (fn pipe [in#] (new ~pipe (strs ~labels)))))

(defn out-e [& labels]
  (edge-step :edge "OutE" labels OutEdgesPipe))

(defn in-e [& labels]
  (edge-step :edge "InE" labels InEdgesPipe))

(defn both-e [& labels]
  (edge-step :edge "BothE" labels BothEdgesPipe))

(defn out [& labels]
  (edge-step :vertex "Out" labels OutPipe))

(defn in [& labels]
  (edge-step :vertex "In" labels InPipe))

(defn both [& labels]
  (edge-step :vertex "Both" labels BothPipe))

(step-type SimpleStep [source-type type name pipe-fn]
           PipeStep
           (build-pipe [step in] (pipe-fn step in)))

(defmacro step [source-type type name pipe]
  `(SimpleStep. ~source-type ~type ~name
          (fn pipe [in#] (new ~pipe))))

(defn out-v []
  (step :edge :vertex "OutV" OutVertexPipe))

(defn in-v []
  (step :edge :vertex "InV" InVertexPipe))

(defn both-v []
  (step :edge :vertex "BothV" BothVerticesPipe))

(defn properties []
  (step #{:vertex :edge} :map "properties" PropertyMapPipe))

(defn labels []
  (step :edge :string "labels" LabelPipe))

