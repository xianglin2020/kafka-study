package store.xianglin.kafka.producer;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class KafkaProducerWithAvro {
    public static void main(String[] args) {
        var bootstrapServers = System.getProperty("bootstrap.servers", "localhost:9092");
        var schemaRegistryUrl = System.getProperty("schema.registry.url");
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put("schema.registry.url", schemaRegistryUrl);
    }
}
