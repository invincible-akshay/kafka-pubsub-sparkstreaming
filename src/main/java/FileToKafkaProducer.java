import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

public class FileToKafkaProducer {

    public static void main(String[] args) throws IOException {
        String filePath = args[0];
        System.out.println("Filepath is: " + filePath);
        FileToKafkaProducer fileToKafkaProducer = new FileToKafkaProducer();
        fileToKafkaProducer.transmitFile(filePath);
    }

    public void transmitFile(String filePath) throws IOException {
        Producer<String, String> producer = KafkaProducerCreator.createProducer();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            int lineCounter = 0;
            while ((line = br.readLine()) != null) {
                // process the line
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(IKafkaConstants.TOPIC_NAME,
                        ""+lineCounter, line);
                // RecordMetadata metadata = producer.send(record).get();
                producer.send(record);
                    /*System.out.println("Record sent with key " + lineCounter + " to partition " + metadata.partition()
                            + " with offset " + metadata.offset());*/
                System.out.println("Record sent with key " + lineCounter);
                lineCounter++;
            }
            producer.close();
        }
    }

    /*
    static void runProducer() {
Producer<Long, String> producer = ProducerCreator.createProducer();
        for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
            ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME,
            "This is record " + index);
            try {
            RecordMetadata metadata = producer.send(record).get();
                        System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
                 }
            catch (ExecutionException e) {
                     System.out.println("Error in sending record");
                     System.out.println(e);
                  }
             catch (InterruptedException e) {
                      System.out.println("Error in sending record");
                      System.out.println(e);
                  }
         }
    }
     */

    public void genericReadFileUsingBufferedReaderStub(String filePath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // process the line
            }
        }
    }

    public void genericReadFileUsingStreamStub(String filePath) {
        /*Path path = Paths.get(getClass().getClassLoader()
                .getResource(filePath).toURI());
        Stream<String> lines = Files.lines(path);
        String data = lines.collect(Collectors.joining("\n"));
        lines.close();*/
        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            stream.forEach(System.out::println);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
