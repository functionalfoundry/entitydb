(ns workflo.entitydb.spec-test
  (:require [clojure.pprint :refer [pprint]]
            [clojure.test :refer [deftest is]]
            [clojure.spec.test.alpha :as stest]
            [workflo.entitydb.core :as entitydb]))


(def check-opts
  {:clojure.spec.test.check/opts
   {:num-tests 25
    :max-size 25}})


#?(:clj
   (deftest check-specs
     (println "")
     (println "Checking symbols with specs:")
     (doseq [sym (sort (stest/checkable-syms))]
       (println "-" sym)
       (when-some [results (stest/check sym check-opts)]
         (doseq [result results]
           (is
            (nil? (get result :failure))
            (str "\n"
                 "Symbol `" (get result :sym) "` failed its spec:\n"
                 (with-out-str (pprint (stest/abbrev-result result))))))))))
