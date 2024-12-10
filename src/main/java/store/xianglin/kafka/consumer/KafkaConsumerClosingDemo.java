package store.xianglin.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerClosingDemo {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerClosingDemo.class);

    public static void main(String[] args) {
        var bootstrapServers = System.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerClosingDemo");

        var consumer = new KafkaConsumer<String, String>(props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            consumer.wakeup();
        }));

        try (consumer) {
            consumer.subscribe(List.of("CustomerCountry"));
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));
                for (var record : records) {
                    log.info("topic = {} partition = {} offset = {} consumer = {} country = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } catch (WakeupException ignored) {
            log.info("wakeupException");
        }
    }
}
