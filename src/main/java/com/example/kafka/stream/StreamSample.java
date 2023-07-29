package com.example.kafka.stream;

import com.example.kafka.admin.AdminSample;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Stream API
 *
 * @author xianglin
 */
public class StreamSample {
    private static final String STREAM_INPUT_TOPIC = "streams-plaintext-input";
    private static final String STREAM_OUT_TOPIC = "streams-plaintext-output";

    private static void createTopic() {
        AdminSample.adminClient(adminClient -> {
            NewTopic outputTopic = new NewTopic(STREAM_OUT_TOPIC, 1, (short) 1);
            NewTopic inputTopic = new NewTopic(STREAM_INPUT_TOPIC, 1, (short) 1);
            CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(inputTopic, outputTopic));
            System.out.println(topics.all().get());
        });
    }

    public static void main(String[] args) throws InterruptedException {
        createTopic();

        CountDownLatch latch = new CountDownLatch(1);

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.114:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "wordCountApp");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaStreams streams = new KafkaStreams(wordCountStream(), properties)) {
            streams.start();
            latch.await();
        }

    }

    /**
     * 定义流计算过程
     */
    private static Topology wordCountStream() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream(STREAM_INPUT_TOPIC);

        KTable<String, Long> count = source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                .groupBy((key, value) -> value)
                .count();

        count.toStream().to(STREAM_OUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
}