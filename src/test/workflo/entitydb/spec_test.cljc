(ns workflo.entitydb.spec-test
  (:require [clojure.pprint :refer [pprint]]
            [clojure.test :refer [deftest is]]
            [clojure.spec.test.alpha :as stest]
            [workflo.entitydb.core :as entitydb]))


(deftest check-specs
  (println "")
  (println "Checkable symbols:")
  (doseq [sym (stest/checkable-syms)]
    (println "-" sym))
  (when-some [results (stest/check)]
    (doseq [result results]
      (is
       (nil? (get result :failure))
       (str "\n"
            "Symbol `" (get result :sym) "` failed its spec:\n"
            (with-out-str (pprint (stest/abbrev-result result))))))))
