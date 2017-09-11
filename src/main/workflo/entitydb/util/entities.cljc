(ns workflo.entitydb.util.entities
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [clojure.walk :refer [walk]]
            [workflo.entitydb.specs.v1 :as specs.v1]
            [workflo.entitydb.util.identity :as identity]))


;;;; Specs


(s/def ::loose? boolean?)
(s/def ::sample? boolean?)
(s/def ::use-spec? boolean?)


(s/def ::empty-set (s/and set? empty?))
(s/def ::empty-vector (s/and vector? empty?))


;;;; Dinstinction between refs and actual entities


(s/fdef ref?
  :args (s/cat :x (s/or :entity ::specs.v1/entity
                        :ref ::specs.v1/ref
                        :map map?
                        :any any?))
  :fn   (s/or :ref
              #(and (= :ref (get-in % [:args :x 0]))
                    (= true (get % :ret)))
              :not-ref
              #(and (not= :ref (get-in % [:args :x 0]))
                    (= false (get % :ret))))
  :ret  boolean?)


(defn ^:export ref?
  ([x]
   (and (map? x)
        (contains? x :workflo/id)
        (= (count x) 1))))


(s/fdef refs?
  :args (s/cat :x (s/or :entities ::specs.v1/entities
                        :refs ::specs.v1/refs
                        :maps (s/coll-of map? :kind vector?)
                        :any any?))
  :fn   (s/or :empty-entities
              #(and (= :entities (get-in % [:args :x 0]))
                    (empty? (get-in % [:args :x 1 1]))
                    (= true (get % :ret)))
              :empty-maps
              #(and (= :maps (get-in % [:args :x 0]))
                    (empty? (get-in % [:args :x 1]))
                    (= true (get % :ret)))
              :refs
              #(and (= :refs (get-in % [:args :x 0]))
                    (= true (get % :ret)))
              :not-refs
              #(and (not= :refs (get-in % [:args :x 0]))
                    (= false (get % :ret))))
  :ret  boolean?)


(defn ^:export refs? [x]
  (and (or (set? x)
           (vector? x))
       (every? ref? x)))


(s/fdef entity?
  :args (s/cat :x (s/or :entity ::specs.v1/entity
                        :loose-entity ::specs.v1/loose-entity
                        :ref ::specs.v1/ref
                        :map map?
                        :any any?)
               :opts (s/? (s/keys :opt-un [::loose? ::use-spec?])))
  :fn   (s/or :entity
              #(and (= :entity (get-in % [:args :x 0]))
                    (= true (get % :ret)))
              :loose-entity
              #(and (= :loose-entity (get-in % [:args :x 0]))
                    (= true (get % [:args :opts :use-spec?]))
                    (= true (get % [:args :opts :loose?]))
                    (= true (get % :ret)))
              :not-entity
              #(= false (get % :ret)))
  :ret  boolean?)


(defn ^:export entity?
  ([x]
   (entity? x {:use-spec? true}))
  ([x {:keys [loose? use-spec?]
       :or   {loose? false
              use-spec? true}}]
   (if use-spec?
     (if loose?
       (s/valid? :workflo.entitydb.specs.v1/loose-entity x)
       (s/valid? :workflo.entitydb.specs.v1/entity x))
     (and (map? x)
          (contains? x :workflo/id)
          (> (count x) 1)))))


(s/fdef entities?
  :args (s/cat :x (s/or :entities ::specs.v1/entities
                        :loose-entities ::specs.v1/loose-entities
                        :refs ::specs.v1/refs
                        :maps (s/coll-of map? :kind vector?)
                        :any any?)
               :opts (s/? (s/keys :opt-un [::loose? ::sample? ::use-spec?])))
  :fn   (s/or :entities
              #(and (= :entities (get-in % [:args :x 0]))
                    (= true (get % :ret)))
              :loose-entities
              #(and (= :loose-entities (get-in % [:args :x 0]))
                    (or (and (= true (get-in % [:args :opts :use-spec?]))
                             (= true (get-in % [:args :opts :loose?])))
                        (= false (get-in % [:args :opts :use-spec?])))
                    (= true (get % :ret)))
              :not-entities
              #(= false (get % :ret)))
  :ret  boolean?)


(defn ^:export entities?
  ([x]
   (entities? x {:use-spec? true}))
  ([x {:keys [loose? sample? use-spec?]
       :or   {loose? false
              sample? false
              use-spec? true}}]
   (and (or (set? x)
            (vector? x))
        (every? #(entity? % {:loose? loose? :use-spec? use-spec?})
                (cond->> x
                  sample? (take 2))))))


;;;; Remove nil attributes


(s/fdef remove-nil-attributes
  :args (s/cat :m map?)
  :fn   (fn [{:keys [args ret]}]
          (and (= (keys (get args :m))
                  (keys ret))
               (every? (fn [k]
                         (if (nil? (get-in args [:m k]))
                           (not (contains? ret k))
                           (= (get-in args [:m k])
                              (get ret k))))
                       (keys (get args :m)))))
  :ret  (s/map-of any? (s/and any? #(not (nil? %)))))


(defn remove-nil-attributes
  [m]
  (into {} (remove (comp nil? second)) m))


;;;; Refifying


(s/fdef entity->ref
  :args (s/cat :entity (s/or :entity ::specs.v1/loose-entity
                             :ref ::specs.v1/ref))
  :ret  ::specs.v1/ref)


(defn ^:export entity->ref [entity]
  (select-keys entity [:workflo/id]))


(s/fdef refify-entity
  :args (s/cat :entity ::specs.v1/loose-entity)
  :fn   (fn [{:keys [args ret]}]
          (let [in-entity  (get args :entity)
                out-entity ret]
            (every? (fn [[k in-val]]
                      (let [out-val (get out-entity k)]
                        (cond
                          (entity? in-val {:use-spec? false}) (ref? out-val)
                          (entities? in-val {:use-spec? false}) (refs? out-val)
                          :else true)))
                    in-entity)))
  :ret  ::specs.v1/entity)


(defn ^:export refify-entity [entity]
  (reduce (fn [out [k v]]
            (assoc out k
                   (cond
                     ;; Single reference attribute
                     (or (entity? v {:use-spec false})
                         (ref? v))
                     (entity->ref v)

                     ;; Multi-reference attribute
                     (and (coll? v)
                          (every? (fn [v*]
                                    (or (entity? v* {:use-spec false})
                                        (ref? v*)))
                                  v))
                     (walk entity->ref identity v)

                     ;; Other attribute
                     :else v)))
          {} entity))


(s/fdef refify-entities
  :args (s/cat :entities ::specs.v1/loose-entities)
  :ret  ::specs.v1/entities)


(defn ^:export refify-entities [entities]
  (into [] (map refify-entity) entities))


;;;; Entity cloning


(defn- substitute-ref-ids [entity id-map]
  (reduce (fn [out [k v]]
            (assoc out k
                   (cond
                     ;; :workflo/id attribute
                     (and (= :workflo/id k)
                          (find id-map v))
                     (get id-map v)

                     ;; Single reference attribute
                     (or (ref? v)
                         (entity? v {:use-spec? true :loose? true}))
                     (update v :workflo/id
                             (fn [id]
                               (or (get id-map (:workflo/id v))
                                   id)))

                     ;; Many-references attribute
                     (or (refs? v)
                         (entities? v {:use-spec? true :loose? true}))
                     (walk (fn [entity]
                             (update entity :workflo/id
                                     (fn [id]
                                       (or (get id-map id)
                                           id))))
                           identity v)

                     ;; Other attribute
                     :else v)))
          {} entity))


(s/fdef clone-entities
  :args (s/cat :entities ::specs.v1/entities)
  :ret  ::specs.v1/entities)


(defn ^:export clone-entities [entities]
  (let [ids    (map :workflo/id entities)
        id-map (zipmap ids (repeatedly identity/make-id))]
    (into #{} (map #(substitute-ref-ids % id-map)) entities)))


;;;; Extracting references


(s/def :workflo.entitydb.extract-references/references
  (s/or :entities ::specs.v1/loose-entities))


(s/fdef extract-references
  :args (s/cat :entity ::specs.v1/loose-entity)
  :ret  (s/keys :req-un [::specs.v1/entity
                         :workflo.entitydb.extract-references/references]))


(defn ^:export extract-references [entity]
  (let [references (transient [])
        entity*    (transient entity)]
    (doseq [[k v] entity]
      (cond
        (entity? v {:use-spec? false})
        (do
          (conj! references v)
          (assoc! entity* k (entity->ref v)))

        (entities? v {:use-spec? false})
        (do
          (doseq [other v]
            (conj! references other))
          (assoc! entity* k (into #{} (map entity->ref) v)))))
    {:entity     (-> entity*
                     (persistent!)
                     (with-meta (meta entity)))
     :references (persistent! references)}))


;;;; Recursive flattening


(s/fdef flatten-entities
  :args (s/cat :entities ::specs.v1/loose-entities)
  :ret  ::specs.v1/entities)


(defn ^:export flatten-entities
  [in-entities]
  (let [out-entities (transient #{})]
    (loop [remaining in-entities]
      (if (empty? remaining)
        (persistent! out-entities)
        (let [entity (first remaining)]
          (let [{:keys [entity references]} (extract-references entity)]
            (conj! out-entities entity)
            (recur (concat (rest remaining) references))))))))


;;;; Deduplication


(s/fdef dedupe-entities
  :args (s/cat :entities ::specs.v1/loose-entities
               :merge-fn (s/? (s/with-gen fn? #(gen/return (comp last vector)))))
  :ret  ::specs.v1/loose-entities)


(defn ^:export dedupe-entities
  ([entities]
   (dedupe-entities entities merge))
  ([entities merge-fn]
   (into [] (map (fn [duplicates]
                   (reduce merge-fn (first duplicates) (rest duplicates))))
         (vals (group-by :workflo/id entities)))))


;;;; Type deduction


(s/fdef entity-name
  :args (s/cat :entity ::specs.v1/loose-entity
               :type-map ::specs.v1/type-map)
  :ret  ::specs.v1/entity-name)


(defn ^:export entity-name
  [entity type-map]
  (first (keep type-map (keys entity))))
