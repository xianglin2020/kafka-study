package com.example.kafka.producer;

import com.example.kafka.ClientConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.Future;

import static com.example.kafka.Constant.TOPIC_NAME;

/**
 * Producer API
 *
 * @author xianglin
 */
@Slf4j
public class ProducerSample {
    public static void main(String[] args) {
        asyncSend();
    }

    /**
     * 异步发送带回调函数
     */
    private static void callbackSend() {
        producer(producer -> {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);
                producer.send(record, (metadata, exception) -> {
                    System.out.println(metadata);

                    if (exception != null) {
                        log.error("", exception);
                    }
                });
            }
        });
    }

    /**
     * 异步阻塞发送
     */
    private static void waitSend() {
        producer(producer -> {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);
                Future<RecordMetadata> metadataFuture = producer.send(record);
                RecordMetadata metadata = metadataFuture.get();
                System.out.println(metadata);
            }
        });
    }

    /**
     * 异步发送
     */
    private static void asyncSend() {
        producer(producer -> {
            for (int i = 0; i < 1000; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);
                producer.send(record);
            }
        });
    }

    /**
     * 创建 Producer
     */
    private static void producer(ClientConsumer<Producer<String, String>> consumer) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.114:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.example.kafka.producer.SamplePartitioner");
        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            consumer.tryAccept(producer);
        }
    }
}