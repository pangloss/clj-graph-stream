(ns pacer.step)

(defn describe-step [step]
  (let [name (:name step)]
    (if (fn? name)
      (name step)
      name)))

(defn check-step [in step]
  (when (not= (:source-type step) (:type in))
    (throw (Exception. (str (describe-step step) " expects type "
                            (:source-type step) " but got " (:type in))))))

