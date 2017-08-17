(ns workflo.entitydb.specs.indexes.vae
  (:require [clojure.spec.alpha :as s]
            [workflo.entitydb.specs.v1 :as specs.v1]
            [workflo.entitydb.specs.indexes.common :as specs.indexes]))


(s/def ::attribute-entity-map
  (s/map-of ::specs.v1/entity-attribute-name
            ::specs.indexes/typed-refs))


(s/def ::value-attribute-entity-map
  (s/map-of ::specs.v1/strict-entity-attribute-value
            ::attribute-entity-map))


(s/def ::index
  ::value-attribute-entity-map)
