import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class FileToKafkaProducer {

    public static void main(String[] args) throws IOException {
        String filePath = args[0];
        System.out.println("Filepath is: " + filePath);
        FileToKafkaProducer fileToKafkaProducer = new FileToKafkaProducer();
        BufferedReader bufferedReader = null;
        if (filePath.endsWith(".csv")) {
            bufferedReader = new BufferedReader(new FileReader(filePath));
            fileToKafkaProducer.transmitFile(bufferedReader);
        } else if (filePath.endsWith(".gz")) {
            InputStreamReader inputStreamReader = new InputStreamReader(new GZIPInputStream(new FileInputStream(filePath)));
            bufferedReader = new BufferedReader(inputStreamReader);
            fileToKafkaProducer.transmitFile(bufferedReader);
        } else {
            System.out.println("Unsupported File Format");
        }
    }

    public void transmitFile(BufferedReader bufferedReader) throws IOException {
        Producer<String, String> producer = KafkaProducerCreator.createProducer();
        try (BufferedReader br = bufferedReader) {
            String line;
            int lineCounter = 0;
            while ((line = br.readLine()) != null) {
                // process the line
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(IKafkaConstants.TOPIC_NAME,
                        ""+lineCounter, line);
                producer.send(record);
                System.out.println("Record sent with key " + lineCounter);
                lineCounter++;
            }
            producer.close();
        }
    }

    public void genericReadFileUsingBufferedReaderStub(String filePath) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                // process the line
            }
        }
    }

    public void genericReadFileUsingStreamStub(String filePath) {
        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            stream.forEach(System.out::println);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
