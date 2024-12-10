package store.xianglin.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class KafkaProducerDemo {
    public static void main(String[] args) throws InterruptedException {
        var property = System.getProperty("bootstrap.servers", "localhost:9092");
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, property);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

//        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducerDemo");
//        props.put(ProducerConfig.ACKS_CONFIG, "all");
//
//        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "");
//        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120");
//        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "");
//        props.put(ProducerConfig.RETRIES_CONFIG, "3");
//        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
//        props.put(ProducerConfig.LINGER_MS_CONFIG, "");
//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "");
//        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
//        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "16384");

        try (var producer = new KafkaProducer<String, String>(props)) {
            var record = new ProducerRecord<>("CustomerCountry", "Precision Products", "France");
            // 发送消息不关心结果
            producer.send(record);

            try {
                // 同步发送
                producer.send(record).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }

            // 异步发送
            producer.send(record, new DemoProducerCallback());

            for (var i = 0; ; i++) {
                producer.send(new ProducerRecord<>("CustomerCountry", "Precision Products" + i, "France" + i));
                TimeUnit.SECONDS.sleep(1);
            }
        }
    }

    private static class DemoProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                throw new RuntimeException(e);
            }
        }
    }
}
