(ns pacer.step)

(defn describe-step [step]
  (let [name (:name step)]
    (if (fn? name)
      (name step)
      name)))

(defn check-step [in step]
  (println "check-step")
  (when-let [rule (:source-type step)]
    (let [type (:type in :unspecified)
          check-fn (if (fn? rule)
                     rule
                     (fn [in] ((if (set? rule) rule (set [rule])) type)))]
      (prn type rule check-fn)
      (when-not (check-fn in)
        (throw (Exception. (str "Step \""(describe-step step) "\" expects type " rule " but got " type)))))))

