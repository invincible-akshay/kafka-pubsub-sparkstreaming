# Read File and Publish line by line as Kafka Messages, Subscribe to topic and ingest to Spark Streaming

## Install and Setup Kafka
Step 1. Install Kafka

On Mac, you can do (if you have homebrew installed): `brew install kafka`

Step 2. Start the zookeper server with this command:
`zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties`

Step 3. Start kafka-server with this command:
`kafka-server-start /usr/local/etc/kafka/server.properties`

If you get connection broken error, then ```vim /usr/local/etc/kafka/server.properties```

Uncomment the line ```listeners=PLAINTEXT://:9092``` and change to ```listeners = listener_name://host_name:port```
replace host_name with 127.0.0.1 and port with 9092

Step 4. Create Kafka Topic

```kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test```

Conversely, you can delete a topic, using this command:

```kafka-topics --zookeeper localhost:2181 --topic test --delete```

## Run the Application
Run the Producer and Consumer classes, i.e. KafkaReceiverSparkStream and FileToKafkaProducer.

If any dependency issues come up, add spark jars as external JARs to the project,
generally found in the location (version may vary): ```apache-spark/2.4.5/libexec/sbin```

## References

- https://medium.com/@Ankitthakur/apache-kafka-installation-on-mac-using-homebrew-a367cdefd273

- https://sparkbyexamples.com/kafka/kafka-delete-topic/

- https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.6.0/bk_kafka-component-guide/content/ch_kafka-development.html

- https://dzone.com/articles/kafka-producer-and-consumer-example

- https://www.tutorialspoint.com/apache_kafka/apache_kafka_simple_producer_example.htm

- https://github.com/simplesteph/medium-blog-kafka-udemy/blob/30c22d7bd04eb7d54e2d7c97cb075ac61a04d283/udemy-reviews-producer/src/main/java/com/github/simplesteph/kafka/producer/udemy/runnable/ReviewsAvroProducerThread.java

- https://medium.com/@stephane.maarek/how-to-use-apache-kafka-to-transform-a-batch-pipeline-into-a-real-time-one-831b48a6ad85

- https://www.baeldung.com/kafka-spark-data-pipeline
