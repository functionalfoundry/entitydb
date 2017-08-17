(ns workflo.entitydb.specs.indexes.common
  (:require [clojure.spec.alpha :as s]
            [workflo.entitydb.specs.v1 :as specs.v1]))


(s/def ::typed-ref
  (s/tuple ::specs.v1/entity-name
           ::specs.v1/entity-id))


(s/def ::typed-refs
  (s/coll-of ::typed-ref :kind set? :min-count 1))
