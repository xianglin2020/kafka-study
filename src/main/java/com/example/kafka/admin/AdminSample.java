package com.example.kafka.admin;

import com.example.kafka.ClientConsumer;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static com.example.kafka.Constant.TOPIC_NAME;

/**
 * Admin API
 *
 * @author xianglin
 */
public class AdminSample {

    public static void main(String[] args) {
        listTopics();
    }

    /**
     * 增加 partition 数量
     */
    private static void incrPartition() {
        adminClient(adminClient -> {
            CreatePartitionsResult partitionsResult = adminClient.createPartitions(Collections.singletonMap(TOPIC_NAME, NewPartitions.increaseTo(2)));
            System.out.println(partitionsResult.all().get());
        });
    }

    /**
     * 修改 Config 信息
     */
    private static void alterConfig() {
        adminClient(adminClient -> {
            ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);

//            Config config = new Config(Collections.singletonList(new ConfigEntry("preallocate", "true")));
//            AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(Collections.singletonMap(configResource, config));
//            System.out.println(alterConfigsResult.all().get());

            AlterConfigOp alterConfigOp = new AlterConfigOp(new ConfigEntry("preallocate", "true"), AlterConfigOp.OpType.SET);
            AlterConfigsResult alterConfigsResult1 = adminClient.incrementalAlterConfigs(Collections.singletonMap(configResource, Collections.singletonList(alterConfigOp)));
            System.out.println(alterConfigsResult1.all().get());
        });
    }

    /**
     * 查看 Config
     */
    private static void describeConfig() {
        adminClient(adminClient -> {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
            DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singletonList(resource));
            System.out.println(describeConfigsResult.all().get());
        });
    }

    /**
     * 描述 Topic
     * <pre>
     * {jz-topic=
     *      (name=jz-topic,
     *      internal=false,
     *      partitions=
     *          (partition=0,
     *          leader=192.168.31.114:9092 (id: 0 rack: null),
     *          replicas=192.168.31.114:9092 (id: 0 rack: null),
     *          isr=192.168.31.114:9092 (id: 0 rack: null)
     *          ),
     *     authorizedOperations=[])
     * }
     * </pre>
     */
    private static void describeTopics() {
        adminClient(adminClient -> {
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(TOPIC_NAME));
            Map<String, TopicDescription> map = describeTopicsResult.all().get();
            System.out.println(map);
        });
    }

    /**
     * 删除 Topic
     */
    private static void deleteTopics() {
        adminClient(adminClient -> {
            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(TOPIC_NAME));
            System.out.println(deleteTopicsResult.all().get());
        });
    }

    /**
     * 获取 Topic 列表
     */
    private static void listTopics() {
        adminClient(adminClient -> {
            ListTopicsOptions options = new ListTopicsOptions();
            // 是否查看 internal topic
            options.listInternal(true);
            ListTopicsResult listTopicsResult = adminClient.listTopics(options);
            listTopicsResult.names().get().forEach(System.out::println);

            listTopicsResult.listings().get().forEach(System.out::println);

            System.out.println(listTopicsResult.namesToListings().get());
        });
    }

    /**
     * 创建 Topic 实例
     */
    private static void createTopic() {
        adminClient(adminClient -> {
            NewTopic newTopic = new NewTopic(TOPIC_NAME, 3, (short) 2);
            CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(newTopic));
            System.out.println(topics.all().get());
        });
    }

    /**
     * 创建 AdminClient
     */
    public static void adminClient(ClientConsumer<AdminClient> consumer) {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.114:9092");

        try (AdminClient adminClient = AdminClient.create(properties)) {
            consumer.tryAccept(adminClient);
        }
    }
}