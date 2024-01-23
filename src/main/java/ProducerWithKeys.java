import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {

    private final static Logger log = LoggerFactory.getLogger(ProducerWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Producer Start");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            String topic = "my-application";
            String key = "id_" + i;
            String value = "value " + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null) {
                        log.info("Received message, " +
                                "\ntopic: " + metadata.topic() +
                                "\npartition: " + metadata.partition() +
                                "\noffset: " + metadata.offset());
                    } else {
                        log.info("ERROR while producing, ", exception);
                    }
                }
            });
        }


        producer.flush();
        producer.close();
    }

}
