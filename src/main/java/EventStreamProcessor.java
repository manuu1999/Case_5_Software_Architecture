import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class EventStreamProcessor {
    private KafkaConsumer<String, String> consumer;

    public EventStreamProcessor() {
        // initialize Kafka consumer
        Properties props = new Properties();
        // set properties
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("driver-position"));
    }

    public void processMessages() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                Utils.GpsPos pos = Utils.extractCoordinates(record.value());
                if (pos != null) {
                    int delay = Utils.requestDelay("driverId", pos);
                    if (delay > 180) {
                        // Handle significant delay
                    }
                }
            }
        }
    }
}
