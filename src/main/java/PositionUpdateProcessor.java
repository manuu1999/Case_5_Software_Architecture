import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class PositionUpdateProcessor {

    private static final String KAFKA_BROKER = "192.168.111.10:9092";
    private static final String POSITION_TOPIC = "driver-position";
    private static final String ROUTE_TIMING_TOPIC = "group1234-route-timing";

    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", KAFKA_BROKER);
        consumerProps.put("group.id", "position-update-consumer");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(POSITION_TOPIC));

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", KAFKA_BROKER);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String line = record.value();
                    Utils.GpsPos gpsPos = Utils.extractCoordinates(line);
                    if (record.key() != null && gpsPos != null) { // Ensuring key and GPS position are not null
                        int delay = Utils.requestDelay(record.key(), gpsPos);
                        String routeTimingEvent = "id: " + record.key() + ", delay: " + delay;
                        producer.send(new ProducerRecord<>(ROUTE_TIMING_TOPIC, record.key(), routeTimingEvent));
                    }
                }
            }
        } finally {
            consumer.close();
            producer.close();
        }

    }
}
