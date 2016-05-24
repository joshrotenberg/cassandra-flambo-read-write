# cassandra-flambo-read-write

Very basic example of using [flambo](https://github.com/yieldbot/flambo) with the [DataStax Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector) 
to read and write data from/to a Cassandra cluster. You'll need a Cassandra instance running locally to test. This just mirrors the functionality found in [BasicReadWriteDemo.scala](https://github.com/datastax/spark-cassandra-connector/blob/master/spark-cassandra-connector-demos/simple-demos/src/main/scala/com/datastax/spark/connector/demo/BasicReadWriteDemo.scala).

See [core.clj](https://github.com/joshrotenberg/cassandra-flambo-read-write/blob/master/src/cassandra_flambo_read_write/core.clj) for details.

## Usage

```
bash$ # start up a local cassandra instance and
bash$ lein run
bash$ # or point it at another cassandra cluster if you want
bash$ lein run 10.0.0.2
```

## License

Copyright © 2016 Josh Rotenberg

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
