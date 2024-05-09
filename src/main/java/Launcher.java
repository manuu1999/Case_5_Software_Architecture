

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

public class Launcher {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-position" + Math.random()); // Unique client ID
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.111.10:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

		final StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> source = builder.stream("driver-position");

		// Applying processing to each event
		source.foreach((key, value) -> {
			System.out.println("MyProcessor got the Event K:" + key + " V:" + value);
		});

		// Mapping each event to a new format or processing outcome
		source.mapValues(value -> "Mapped Event: " + value);

		// Building the topology
		final Topology topology = builder.build();

		// Creating KafkaStreams instance with the topology and properties
		final KafkaStreams streams = new KafkaStreams(topology, props);

		final CountDownLatch latch = new CountDownLatch(1);

		// Attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();  // Wait until latch is decremented
		} catch (Throwable e) {
			System.exit(1);
		}

		System.exit(0);
	}


}
