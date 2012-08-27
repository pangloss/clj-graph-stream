(ns pacer
    (:use [clojure.pprint :only [pprint]]
          pacer.step
     ))

(defn simple-encoder [] { :encode nil :decode nil })

(defn show
  "Show an easy to read"
  [route]
  (->> route
       (map str)
       (clojure.string/join " -> ")))

(defn- pipe-from-step [in step]
       (cond
         (satisfies? pacer.step/BuildPipe step) (doto (build-pipe step in)
                                          (.setStarts (:pipe in (:source in))))
         (satisfies? pacer.step/BuildIterator step) (iterator step (:pipe in (:source in)))
         :else (throw (Exception. "Don't know how to build step"))))

(defn build-pipeline [[source & route]]
  (if route
    (reduce (fn [in step]
                (check step in)
                { :pipe (pipe-from-step in step)
                  :type (:type step (:type in))
                  :route (conj (:route in) step)})
            { :source source, :type (:type source), :route [] }
            route)
    source))

(defn pipe
  "Build a pipe from a route definition"
  [route]
  (:pipe (build-pipeline route)))

(defmacro route [& steps]
  (vec (map (fn [step]
                `(let [step# ~step]
                   (if (map? step#)
                     step#
                     ~(list step))))
            steps)))


(comment
  (route g v (loop (out-e :x) in-v :max 5 :while ??))
  (route g v (loop { :max 5 } (out-e :x) in-v))
  (route g v (loop
               (fn [v path loop emit] (loop) (emit path))
               ; fn could use CPS. would that cause stack overflow?
               ; we could cause the continuations to enqueue the elemnent and return, not actually continue
               ; cps advantage is that I could use args to emit other things than simply the current element if I gave it args.
               :emit-if #() ; return t/f
               :loop-if #() ; return t/f
               :max-depth 4 ; would not call loop-if when at max
               :min-depth 2 ; would not emit or call emit-if
               (out-e :x) in-v))
  (route g v (branch { :a (v out-e) :b (v in out)} ) (merge :a :b))
  (route g v (lookahead out { :name "Frank" } (in :knows) #{ some-vertex }))

  )
