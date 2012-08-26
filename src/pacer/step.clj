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
(defprotocol PacerStep
             (check [step in]))

(defprotocol IteratorStep
             (iterator [step in]))

(defprotocol PipeStep
             (build-pipe [step in]))

(declare check-step)

(defmacro step-type [name args & forms]
  `(defrecord ~name ~args
              Object
              (toString [step#] (show-step step#))
              PacerStep
              (check [step# in#] (check-step step# in#))
              ~@forms))
