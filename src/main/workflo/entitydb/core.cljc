(ns workflo.entitydb.core
  (:require [clojure.spec.alpha :as s]
            [workflo.entitydb.specs.v1]))


;;;; Create a db


(s/fdef empty-db
  :args nil?
  :ret :workflo.entitydb/v1)


(defn empty-db []
  {:workflo.entitydb.v1/data {}
   :workflo.entitydb.v1/indexes {}})
