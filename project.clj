(defproject eyvind "0.1.0-SNAPSHOT"
  :description "Eyvind is an experimental distributed rule engine written in Clojure"
  :url "http://hraberg.github.io/eyvind/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0-alpha3"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.zeromq/jeromq "0.3.5"]
                 [org.zeromq/cljzmq "0.1.4" :exclusions [org.zeromq/jzmq]]]
  :profiles {:dev {:dependencies [[org.clojure/core.async "0.1.346.0-17112a-alpha"]
                                  [com.taoensso/nippy "2.9.0"]]}}
  :global-vars {*warn-on-reflection* true
                *unchecked-math* :warn-on-boxed}
  :jvm-opts ^:replace [])
