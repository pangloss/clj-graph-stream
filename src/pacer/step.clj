(ns pacer.step)


(defn show-step [step]
       (let [name (:name step)]
         (if (fn? name)
           (name step)
           name)))

(defn check-step [step in]
  (when-let [rule (:source-type step)]
    (let [type (:type in :unspecified)
          check-fn (if (fn? rule)
                     rule
                     (fn [in] ((if (set? rule) rule (set [rule])) type)))]
      (when-not (check-fn in)
        (throw (Exception. (str "Step \"" step "\" expects type " rule " but got " type " from " in)))))))

; Signature of check and pipe are the reverse of reducer fns...
(defprotocol Step
             (check [step in]))

(defprotocol BuildIterator
             (iterator [step in]))

(defprotocol BuildPipe
             (build-pipe [step in]))

(defmacro defstep [name args & forms]
  `(defrecord ~name ~args
              Object
              (toString [step#] (show-step step#))
              Step
              (check [step# in#] (check-step step# in#))
              ~@forms))
