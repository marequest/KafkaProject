import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.index.mapper.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Consumer {

    private final static Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Consumer Start");

        String groupId = "wikimedia.recentchange";
        String topic = "wikimedia.recentchange";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(List.of(topic));

        String filePath = "output.csv";
        BufferedWriter bufferedWriter = null;
        try {
            FileWriter fileWriter = new FileWriter(filePath, true);
            bufferedWriter = new BufferedWriter(fileWriter);

            while (true) {
                log.info("Polling...");

                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(5000));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    String line = "Key: " + record.key() + " Value: " + record.value() +
                            " Partition: " + record.partition() + " Offset: " + record.offset();
                    String jsonValue = record.value();
                    String csvLine = convertJsonToCsv(jsonValue);

                    bufferedWriter.write(csvLine);
                    bufferedWriter.newLine();
                }
                bufferedWriter.flush();

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (bufferedWriter != null) {
                    bufferedWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private static String convertJsonToCsv(String jsonValue) {
        JsonObject jsonObject = JsonParser.parseString(jsonValue).getAsJsonObject();
        // Extract values from JSON and format them as a CSV line
        String wiki = jsonObject.get("wiki").getAsString();
        Integer timestamp = jsonObject.get("timestamp").getAsInt();
        String user = jsonObject.get("user").getAsString();
        String type = jsonObject.get("type").getAsString();
        String bot = jsonObject.get("bot").getAsString();
        String title = jsonObject.get("title").getAsString();
        String comment = jsonObject.get("comment").getAsString();
        Integer lengthOld = null, lengthNew = null, revisionOld = null, revisionNew = null;
        if (jsonObject.has("length")){
            lengthOld = jsonObject.getAsJsonObject("length").has("old") ? jsonObject.getAsJsonObject("length").get("old").getAsInt() : null;
            lengthNew = jsonObject.getAsJsonObject("length").has("new") ? jsonObject.getAsJsonObject("length").get("new").getAsInt() : null;
        }
        if (jsonObject.has("revision")) {
            revisionOld = jsonObject.getAsJsonObject("revision").has("old") ? jsonObject.getAsJsonObject("revision").get("old").getAsInt() : null;
            revisionNew = jsonObject.getAsJsonObject("revision").has("new") ? jsonObject.getAsJsonObject("revision").get("new").getAsInt() : null;
        }

        return wiki + "," + timestamp + "," + user + "," + type + "," + bot + "," +
                title + "," + comment + "," + lengthOld + "," + lengthNew + "," + revisionOld + "," + revisionNew;
    }
}






