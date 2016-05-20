(ns cassandra-flambo-read-write.core
  (:require [qbits.alia :as alia])
  (:require [flambo.api :as f]
            [flambo.conf :as conf]
            [flambo.tuple :as ft]
            [clojure.string :as s])
  (:require [clojure.tools.logging :as log])
  (:require [clojure.walk :refer [keywordize-keys]])
  (:import [com.datastax.spark.connector.japi CassandraJavaUtil]))

(defn init-cassandra
  [& hosts]
  (let [cluster (alia/cluster {:contact-points (or hosts ["localhost"])})
        session (alia/connect cluster)]
    (alia/execute session "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    (alia/execute session "CREATE TABLE IF NOT EXISTS test.key_value (key INT PRIMARY KEY, value VARCHAR)")
    (alia/execute session "TRUNCATE test.key_value")
    (alia/execute session "INSERT INTO test.key_value(key, value) VALUES (1, 'first row')")
    (alia/execute session "INSERT INTO test.key_value(key, value) VALUES (2, 'second row')")
    (alia/execute session "INSERT INTO test.key_value(key, value) VALUES (3, 'third row')")

    ;(alia/shutdown session)
    (alia/shutdown cluster)
    ))

(defn -main
  [& args]
  (let [c (-> (conf/spark-conf)
              (conf/master "local[2]")
              (conf/app-name "cassandra-flambo-read-write")
              (conf/set "spark.cassandra.connection.host" "localhost"))
        sc (f/spark-context c)]
    ;; create the keyspace and table, and populate it
    (init-cassandra)

    (-> (CassandraJavaUtil/javaFunctions sc)
        (.cassandraTable "test" "key_value")
        (.select (into-array String ["key" "value"]))
        (f/map (f/fn [r] (keywordize-keys (into {} (.toMap r)))))
        f/collect
        clojure.pprint/pprint
;        (f/foreach (f/fn [x] (println x)))
        )

    (comment
      (-> (f/text-file sc "resources/words")
          (f/flat-map (f/fn [l] (s/split l #" ")))
          (f/map-to-pair (f/fn [w] (ft/tuple w 1)))
          (f/reduce-by-key (f/fn [x y] (+ x y)))
          (CassandraJavaUtil/javaFunctions)
          (.writerBuilder "test" "words" (CassandraJavaUtil/mapTupleToRow (class "foo") (class 1)))
          (.withColumnSelector (CassandraJavaUtil/someColumns (into-array String ["word" "count"])))   
          .saveToCassandra))
    (comment
      (-> (CassandraJavaUtil/javaFunctions sc)
          (.cassandraTable "test" "words")
          (.select (into-array String ["count" "word"]))
                                        ;        (f/foreach (f/fn [r] (println (.toMap r))))
          (f/map (f/fn [r] (keywordize-keys (into {}  (.toMap r)))))
                                        ;        f/collect
                                        ;        (f/foreach (f/fn [r] (println (.fieldNames r))))
                                        ;.cassandraCount
          clojure.pprint/pprint
          ))
    
))
