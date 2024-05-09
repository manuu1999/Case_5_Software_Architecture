import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class DelayChecker {

    private static final String KAFKA_BROKER = "192.168.111.10:9092";
    private static final String ROUTE_TIMING_TOPIC = "group1234-route-timing";

    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", KAFKA_BROKER);
        consumerProps.put("group.id", "delay-checker-consumer");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(ROUTE_TIMING_TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    Integer delay = Utils.extractDelay(record.value());
                    if (delay != null && delay > 180) { // 3 minutes in seconds
                        System.out.println("Significant delay detected for delivery ID: " + record.key());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}
