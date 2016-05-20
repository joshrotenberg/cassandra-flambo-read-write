(defproject cassandra-flambo-read-write "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [yieldbot/flambo "0.7.1"]
                 [com.datastax.spark/spark-cassandra-connector-java_2.10 "1.5.0"]
                 [cc.qbits/alia-all "3.1.5"]
                 [org.clojure/tools.cli "0.3.3"]
                 [org.clojure/tools.logging "0.3.1"]]
  :main cassandra-flambo-read-write.core
  :profiles {:provided {:dependencies [[org.apache.spark/spark-core_2.10 "1.6.1"]
                                       [org.apache.spark/spark-sql_2.10 "1.6.1"]]}
             :dev {:aot [cassandra-flambo-read-write.core]}})
