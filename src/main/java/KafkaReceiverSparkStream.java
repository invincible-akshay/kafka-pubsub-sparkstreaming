import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class KafkaReceiverSparkStream {

    public static void main(String[] args) throws InterruptedException {
        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("CountingApp");
        JavaStreamingContext streamingContext = new JavaStreamingContext(
                sparkConf, Durations.seconds(1));

        Map<String, Object> kafkaParams = new HashMap<>();
        Properties props = KafkaConsumerCreator.getConsumerProperties();
        for (final String name: props.stringPropertyNames())
            kafkaParams.put(name, props.getProperty(name));

        Consumer<String, String> consumer = KafkaConsumerCreator.createConsumer();
        Collection<String> topics = Arrays.asList("test");

        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

        // Process Input Stream
        JavaPairDStream<String, String> results = messages
                .mapToPair(
                        record -> new Tuple2<>(record.key(), record.value())
                );
        JavaDStream<String> lines = results
                .map(
                        tuple2 -> tuple2._2()
                );

        JavaPairDStream<String, Integer> itemCounts = lines
                .mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String t) throws Exception {
                        String[] words = t.split(",");
                        return new Tuple2<String, Integer>(words[1], 1);
                    }
                }).reduceByKey(
                        (i1, i2) -> i1 + i2
                );
        itemCounts.print();

        streamingContext.start();               // Start the computation
        streamingContext.awaitTermination();    // Wait for the computation to terminate
    }

}
