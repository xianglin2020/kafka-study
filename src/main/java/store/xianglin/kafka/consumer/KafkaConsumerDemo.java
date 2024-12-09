package store.xianglin.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerDemo.class);

    public static void main(String[] args) {
        var bootstrapServers = System.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "CountryCounter");

        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "200");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "500000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaConsumerDemo");

        try (var consumer = new KafkaConsumer<String, String>(props)) {
            // 订阅主题
            consumer.subscribe(List.of("CustomerCountry"));

            var timeout = Duration.ofMillis(100);
            while (true) {
                var records = consumer.poll(timeout);

                for (final var record : records) {
                    log.info("topic = {} partition = {} offset = {} consumer = {} country = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
            }

        }
    }
}
