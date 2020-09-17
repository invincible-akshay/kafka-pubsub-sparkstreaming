public interface IKafkaConstants {
    public static String KAFKA_BROKERS = "localhost:9092";
    public static String TOPIC_NAME = "test";
    public static String GROUP_ID_CONFIG = "consumerGroup2";
    public static String OFFSET_RESET_EARLIER = "earliest";
    public static String ACKS_CONFIG = "all";
    public static Integer RETRIES_CONFIG = 0;

    // Additional Optional Arguments
    /*public static String STRING_DESERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.StringDeserializer";
    public static Integer MESSAGE_COUNT = 1000;
    public static String CLIENT_ID = "client1";
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;
    public static String OFFSET_RESET_LATEST = "latest";
    public static Integer MAX_POLL_RECORDS = 1;*/

}