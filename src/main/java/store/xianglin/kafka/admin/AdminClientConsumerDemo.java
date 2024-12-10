package store.xianglin.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class AdminClientConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(AdminClientConsumerDemo.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var bootstrapServers = System.getProperty("bootstrap.servers", "localhost:9092");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        try (var admin = AdminClient.create(props)) {
            // 列出所有消费者组
            admin.listConsumerGroups().valid().get().forEach(System.out::println);

            // KafkaConsumerReBalanceDemo 消费者组详情
            var describeConsumerGroupsResult = admin.describeConsumerGroups(List.of("KafkaConsumerReBalanceDemo"));
            var groupDescription = describeConsumerGroupsResult.describedGroups().get("KafkaConsumerReBalanceDemo").get();
            System.out.println(groupDescription);


            // 获取消费者组读取的所有主题和分区
            var offsets = admin.listConsumerGroupOffsets("KafkaConsumerReBalanceDemo").partitionsToOffsetAndMetadata("KafkaConsumerReBalanceDemo").get();

            // 希望得到每个分区的最后一条消息的偏移量
            var requestLatestOffsets = new HashMap<TopicPartition, OffsetSpec>();
            for (final var entry : offsets.entrySet()) {
                requestLatestOffsets.put(entry.getKey(), OffsetSpec.latest());
            }
            var latestOffsets = admin.listOffsets(requestLatestOffsets).all().get();
            for (final var entry : offsets.entrySet()) {
                var topicPartition = entry.getKey();
                var commitOffset = entry.getValue().offset();

                var latestOffset = latestOffsets.get(topicPartition).offset();

                log.info("topic = {}, partition = {}, commitOffset = {} latestOffset = {}", topicPartition.topic(), topicPartition.partition(), commitOffset, latestOffset);
            }


            // 尝试将所有偏移量重置
            // 1. 获取消费者组读取的分区的初始偏移量
            var offsetAndMetadataMap = admin.listConsumerGroupOffsets("KafkaConsumerReBalanceDemo").partitionsToOffsetAndMetadata("KafkaConsumerReBalanceDemo").get();
            var requestMap = offsetAndMetadataMap.keySet().stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.earliest()));
            var offsetsResultInfoMap = admin.listOffsets(requestMap).all().get();
            // 2. 尝试重置偏移量
            var map = new HashMap<TopicPartition, OffsetAndMetadata>();
            for (final var entry : offsetsResultInfoMap.entrySet()) {
                map.put(entry.getKey(), new OffsetAndMetadata(entry.getValue().offset()));
            }
            // 没有停止消费者组时修改偏移量会抛出异常：
            // Caused by: org.apache.kafka.common.errors.UnknownMemberIdException:
            // Failed altering consumer group offsets for the following partitions: [CustomerCountry-0, CustomerCountry-1, CustomerCountry-2]
            admin.alterConsumerGroupOffsets("KafkaConsumerReBalanceDemo", map).all().get();
        }

    }
}
