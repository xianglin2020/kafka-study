package store.xianglin.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;


public class KafkaConsumerReBalanceDemo {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerReBalanceDemo.class);

    public static void main(String[] args) {
        var bootstrapServers = System.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerReBalanceDemo");

        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of("CustomerCountry"), new KafkaConsumerReBalanceListener());

            // 从特定偏移量读取数据
            var earlier = Instant.now().atZone(ZoneId.systemDefault()).minusSeconds(650).toEpochSecond();
            var map = consumer.assignment().stream().collect(Collectors.toMap(tp -> tp, tp -> earlier));
            // 通过时间戳获取偏移量
            var offsetMap = consumer.offsetsForTimes(map);
            for (final var entry : offsetMap.entrySet()) {
                // 设置偏移量
                consumer.seek(entry.getKey(), entry.getValue().offset());
            }

            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));
                for (var record : records) {
                    log.info("topic = {} partition = {} offset = {} consumer = {} country = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
            }
        }
    }

    private static class KafkaConsumerReBalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.info("onPartitionsRevoked: {}", partitions);

        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.info("onPartitionsAssigned: {}", partitions);
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            ConsumerRebalanceListener.super.onPartitionsLost(partitions);
        }
    }
}
