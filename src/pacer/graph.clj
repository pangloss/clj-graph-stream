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
          [pacer.step :only [defstep]]))

(defprotocol PacerGraph
             (create-vertex [graph])
             (create-edge [graph label from to])
             (encode [graph value])
             (decode [graph value]))

(defrecord Vertex [graph element])
(defrecord Edge [graph element])

(defrecord Graph [type name raw-graph encoder]
           Object
           (toString [g] (str @(:raw-graph g)))
           pacer.step/Step
           (check [g in])
           PacerGraph
           (create-vertex [graph]
                          (Vertex. graph (.addVertex @(:raw-graph graph) nil)))
           (create-edge [graph label from to]
                        (Edge. graph (.addEdge @(:raw-graph graph) nil (:element from) (:element to) (str label))))
           (encode [graph value] ((:encode encoder) value))
           (decode [graph value] ((:decode encoder) value)))



(defstep IteratorStep [source-type type name iterator]
               pacer.step/BuildIterator
               (iterator [s in] (iterator s in)))

(defstep EdgeStep [source-type type name labels pipe-fn]
               pacer.step/BuildPipe
               (build-pipe [step in] (pipe-fn step in)))

(defstep SimpleStep [source-type type name pipe-fn]
               pacer.step/BuildPipe
               (build-pipe [step in] (pipe-fn step in)))

(defmacro edge-step [type name labels pipe]
  `(EdgeStep. :vertex ~type (name+ ~name ~labels) ~labels
              (fn pipe [step# in#] (new ~pipe (strs ~labels)))))

(defmacro step [source-type type name pipe]
  `(SimpleStep. ~source-type ~type ~name
          (fn pipe [step# in#] (new ~pipe))))



(defn tg []
  ; seems awkward to have the first argument be :graph but I don't know
  ; a way for records to have default values.
  (Graph. :graph "TinkerGraph"
          (atom (com.tinkerpop.blueprints.impls.tg.TinkerGraph.))
          (atom (pacer/simple-encoder))))


(defn v []
  (IteratorStep. :graph :vertex "V"
              (fn iterator [step source]
                  (.. @(:raw-graph source) getVertices iterator))))

(defn e []
  (IteratorStep. :graph :edge "E"
              (fn iterator [step source]
                  (.. @(:raw-graph source) getEdges iterator))))

(defn- name+ [name labels]
       (if (empty? labels)
         name
         (str name " (" (join ", " labels) ")")))

(defn- strs [args]
       (into-array String (map str args)))


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

