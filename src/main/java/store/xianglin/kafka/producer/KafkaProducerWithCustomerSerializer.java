package store.xianglin.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class KafkaProducerWithCustomerSerializer {
    public static void main(String[] args) {
        var property = System.getProperty("bootstrap.servers", "localhost:9092");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, property);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "store.xianglin.kafka.producer.CustomerSerializer");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "store.xianglin.kafka.producer.BananaPartitioner");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "store.xianglin.kafka.producer.CountingProducerInterceptor");
        try (var producer = new KafkaProducer<String, Customer>(props)) {
            for (Customer customer : new CustomerGenerator()) {
                var record = new ProducerRecord<>("CustomerName", "Banana", customer);
                record.headers().add("privacy-level", "YOLO".getBytes(StandardCharsets.UTF_8));
                producer.send(record);
            }
        }
    }
}
