package com.example.kafka.consumer;

import com.example.kafka.ClientConsumer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.util.CollectionUtils;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.example.kafka.Constant.TOPIC_NAME;

/**
 * Consumer API
 *
 * @author xianglin
 */
public class ConsumerSample {
    public static void main(String[] args) {
        seek();
    }

    /**
     * 指定 consumer offset
     */
    private static void seek() {
        consumer(consumerClient -> {
            // 订阅指定partition
            TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);
            consumerClient.assign(Collections.singletonList(topicPartition));

            long offset = 0;
            while (true) {
                consumerClient.seek(topicPartition, offset);
                ConsumerRecords<String, String> consumerRecords = consumerClient.poll(Duration.ofMillis(10000));
                consumerRecords.forEach(System.out::println);
                offset = consumerRecords.count() + offset;
            }
        });
    }

    /**
     * 多个eventHandler完成多线程
     */
    private static void eventHandlerThread() {
        consumer(consumerClient -> {
            ExecutorService executorService = Executors.newCachedThreadPool();
            consumerClient.subscribe(Collections.singletonList(TOPIC_NAME));
            ConsumerRecords<String, String> consumerRecords = consumerClient.poll(Duration.ofMillis(10000));
            consumerRecords.forEach(r ->
                    executorService.execute(() -> {
                        System.out.println(Thread.currentThread().getName());
                        System.out.println(r);
                    }));
            consumerClient.commitAsync();
        });
    }

    /**
     * 多个consumer多线程
     */
    private static void consumerThread() {
        int consumerSize = 2;
        ExecutorService executorService = Executors.newFixedThreadPool(consumerSize);
        for (int i = 0; i < consumerSize; i++) {
            int partition = i;
            executorService.execute(() ->
                    consumer(consumerClient -> {
                        TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, partition);
                        consumerClient.assign(Collections.singletonList(topicPartition));
                        ConsumerRecords<String, String> consumerRecords = consumerClient.poll(Duration.ofMillis(10000));
                        List<ConsumerRecord<String, String>> records = consumerRecords.records(topicPartition);
                        records.forEach(r -> {
                            System.out.println(Thread.currentThread().getName());
                            System.out.println(r);
                        });
                        long offset = records.get(records.size() - 1).offset();
                        consumerClient.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset + 1)));
                    }));
        }
    }

    /**
     * 订阅指定 partition
     */
    private static void assign() {
        consumer(consumerClient -> {
            // 订阅指定partition
            TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);
            consumerClient.assign(Collections.singletonList(topicPartition));

            ConsumerRecords<String, String> consumerRecords = consumerClient.poll(Duration.ofMillis(10000));
            List<ConsumerRecord<String, String>> records = consumerRecords.records(topicPartition);
            records.forEach(System.out::println);
            long offset = records.get(records.size() - 1).offset();
            consumerClient.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset + 1)));
        });
    }

    /**
     * 按partition分别处理，单独提交 consumer offset
     */
    private static void dealAndCommitByPartition() {
        consumer(consumerClient -> {
            consumerClient.subscribe(Collections.singletonList(TOPIC_NAME));

            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumerClient.poll(Duration.ofMillis(10000));
                // 按 partition 处理
                for (TopicPartition topicPartition : consumerRecords.partitions()) {
                    List<ConsumerRecord<String, String>> records = consumerRecords.records(topicPartition);
                    if (!CollectionUtils.isEmpty(records)) {
                        records.forEach(System.out::println);

                        // 按 partition 提交
                        long offset = records.get(records.size() - 1).offset();
                        consumerClient.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset + 1)));
                    }
                }
            }
        });
    }

    /**
     * consumer 消费所有 partition
     */
    private static void poll() {
        consumer(clientConsumer -> {
            clientConsumer.subscribe(Collections.singletonList(TOPIC_NAME));

            while (true) {
                ConsumerRecords<String, String> consumerRecords = clientConsumer.poll(Duration.ofMillis(10000));
                consumerRecords.forEach(System.out::println);
                // 手动提交 consumer offset：enable.auto.commit = false
                clientConsumer.commitAsync();
            }
        });
    }

    private static void consumer(ClientConsumer<Consumer<String, String>> clientConsumer) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.114:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        try (Consumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            clientConsumer.tryAccept(consumer);
        }
    }
}