(ns pacer.step)

(defn describe-step [step]
  (:name step))

(defn check-step [in step]
  (when (not= (:source-type step) (:type in))
    (throw (Exception. (str (describe-step step) " expects type "
                            (:source-type step) " but got " (:type in))))))

