#!/usr/bin/env boot


(def +project+ 'workflo/entitydb)
(def +version+ "0.1.8")


(set-env!
 :resource-paths #{"src/main"}
 :dependencies '[;; Boot setup
                 [adzerk/boot-cljs "2.0.0" :scope "test"]
                 [adzerk/boot-reload "0.5.1" :scope "test"]
                 [adzerk/boot-test "1.2.0" :scope "test"]
                 [adzerk/bootlaces "0.1.13" :scope "test"]
                 [boot-codox "0.10.3" :scope "test"]
                 [crisptrutski/boot-cljs-test "0.3.0" :scope "test"]

                 ;; General dependencies
                 [org.clojure/spec.alpha "0.1.123"]
                 [org.clojure/test.check "0.9.0" :scope "test"]
                 [inflections "0.13.0"]
                 [org.clojure/clojure "1.9.0-alpha17"]
                 [org.clojure/clojurescript "1.9.671"]

                 ;; Workflo dependencies
                 [workflo/macros "0.2.63"]])


(require '[adzerk.boot-cljs :refer [cljs]]
         '[adzerk.boot-reload :refer [reload]]
         '[adzerk.boot-test :refer [test] :rename {test test-clj}]
         '[adzerk.bootlaces :refer :all]
         '[boot.git :refer [last-commit]]
         '[codox.boot :refer [codox]]
         '[crisptrutski.boot-cljs-test :refer [test-cljs]])


(bootlaces! +version+ :dont-modify-paths? true)


(task-options!
 test-cljs {:js-env :phantom
            :update-fs? true
            :exit? true
            :optimizations :none}
 push      {:repo "deploy-clojars"
            :ensure-branch "master"
            :ensure-clean true
            :ensure-tag (last-commit)
            :ensure-version +version+}
 pom       {:project +project+
            :version +version+
            :description "Database for entities defined with workflo/macros"
            :url "https://github.com/workfloapp/entitydb"
            :scm {:url "https://github.com/workfloapp/entitydb"}
            :license {"MIT License" "https://opensource.org/licenses/MIT"}})


(deftask build-dev
  []
  (comp (cljs :source-map true
              :optimizations :none
              :compiler-options {:devcards true
                                 :parallel-build true})))


(deftask build-production
  []
  (comp (cljs :optimizations :advanced
              :compiler-options {:devcards true
                                 :parallel-build true})))


(deftask testing
  []
  (merge-env! :source-paths #{"src/test"})
  identity)


(deftask docs
  []
  (comp (codox :name "workflo/entitydb"
               :source-paths #{"src/main"}
               :output-path "docs"
               :metadata {:doc/format :markdown})
        (target)))


(deftask test
  []
  (comp (testing)
        (test-cljs)
        (test-clj)))


(deftask install-local
  []
  (comp (pom)
        (jar)
        (install)))


(deftask deploy-snapshot
  []
  (comp (pom)
        (jar)
        (push-snapshot)))


(deftask deploy-release
  []
  (comp (pom)
        (jar)
        (push-release)))
