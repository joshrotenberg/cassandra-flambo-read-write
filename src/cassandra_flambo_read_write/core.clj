(ns cassandra-flambo-read-write.core
  (:require [qbits.alia :as alia])
  (:require [flambo.api :as f]
            [flambo.conf :as conf]
            [flambo.tuple :as ft]
            [clojure.string :as s])
  (:require [clojure.tools.logging :as log])
  (:require [clojure.string :refer [join]])
  (:require [clojure.walk :refer [keywordize-keys]])
  (:import [com.datastax.spark.connector.japi CassandraJavaUtil]))

(defn init-cassandra
  "Set up the Cassandra keyspace and table, and populate with some initial data."
  [hosts]
  (let [cluster (alia/cluster {:contact-points (or hosts ["localhost"])})
        session (alia/connect cluster)]

    (alia/execute session "CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    (alia/execute session "CREATE TABLE IF NOT EXISTS test.key_value (key INT PRIMARY KEY, value VARCHAR)")

    (alia/execute session "TRUNCATE test.key_value")

    (alia/execute session "INSERT INTO test.key_value(key, value) VALUES (1, 'first row')")
    (alia/execute session "INSERT INTO test.key_value(key, value) VALUES (2, 'second row')")
    (alia/execute session "INSERT INTO test.key_value(key, value) VALUES (3, 'third row')")

    (alia/shutdown session)
    (alia/shutdown cluster)))

(defn select
  "Takes a SparkContext, Cassandra keyspace and table name, and a list of column names to select. Returns the resulting RDD of keywordized name/value rows."
  [sc keyspace table columns]
  (-> (CassandraJavaUtil/javaFunctions sc)
      (.cassandraTable keyspace table)
      (.select (into-array String columns))
      (f/map (f/fn [r] (keywordize-keys (into {} (.toMap r)))))))

(defn save-to-cassandra
  "Takes a SparkContext, Cassandra keyspace and table name, a sequence of columns and a sequence of Tuple2s (ugh) to insert."
  [sc keyspace table columns tuple2s]
  (-> (f/parallelize sc tuple2s)
      (CassandraJavaUtil/javaFunctions)
      (.writerBuilder keyspace table (CassandraJavaUtil/mapTupleToRow (class (._1 (first tuple2s))) (class (._2 (first tuple2s)))))
      (.withColumnSelector (CassandraJavaUtil/someColumns (into-array String columns)) )
      .saveToCassandra))

(defn -main
  [& args]
  (let [hosts (if (nil? args) "localhost" (join "," args))
        c (-> (conf/spark-conf)
              (conf/master "local[2]")
              (conf/app-name "cassandra-flambo-read-write")
              (conf/set "spark.cassandra.connection.host" hosts))
        sc (f/spark-context c)]

    ;; create the keyspace and table, and populate it
    (init-cassandra args)

    ;; read what's in there and print it out
    (-> (select sc "test" "key_value" ["key" "value"])
        f/collect
        clojure.pprint/pprint)

    ;; save two more rows
    (save-to-cassandra sc "test" "key_value" ["key" "value"] [(ft/tuple 4 "fourth row") (ft/tuple 5 "fifth row")])

    ;; read what's in there and print it out again, should be five rows now
    (-> (select sc "test" "key_value" ["key" "value"])
        f/collect
        clojure.pprint/pprint)))


