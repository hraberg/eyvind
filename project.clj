(defproject eyvind "0.1.0-SNAPSHOT"
  :description "Eyvind is an experimental distributed rule engine written in Clojure"
  :url "http://hraberg.github.io/eyvind/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0-RC1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.zeromq/jeromq "0.3.4"]
                 [org.zeromq/cljzmq "0.1.4" :exclusions [org.zeromq/jzmq]]]
  :profiles {:dev {:dependencies [[org.clojure/core.async "0.1.346.0-17112a-alpha"]
                                  [clojure-msgpack "1.1.0" :exclusions [org.clojure/test.check]]]}}
  :global-vars {*warn-on-reflection* true
                *unchecked-math* :warn-on-boxed}
  :java-source-paths ["src"]
  :javac-options ["-XDignore.symbol.file"]
  :jvm-opts ^:replace [])
