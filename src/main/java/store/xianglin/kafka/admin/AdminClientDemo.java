package store.xianglin.kafka.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminClientDemo {
    private final static Logger log = LoggerFactory.getLogger(AdminClientDemo.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var bootstrapServers = System.getProperty("bootstrap.servers", "localhost:9092");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        var admin = AdminClient.create(props);

        String topic = "test";

        var listTopicsResult = admin.listTopics();
        listTopicsResult.names().get().forEach(System.out::println);

        try {
            var describeTopicsResult = admin.describeTopics(List.of(topic));
            var topicDescription = describeTopicsResult.allTopicNames().get().get(topic);
            log.info("topicDescription: {}", topicDescription);
        } catch (Exception exception) {
            // Future 中的异常由 java.util.concurrent.ExecutionException 提供
            if (exception.getCause() instanceof UnknownTopicOrPartitionException) {
                log.info("topic {} not found", topic);

                // 创建 Topic
                var createTopicsResult = admin.createTopics(List.of(new NewTopic(topic, 10, (short) 1)));
                createTopicsResult.all().get();

                assert admin.listTopics().names().get().contains(topic);
            } else {
                throw new RuntimeException(exception);
            }
        }

        // 删除 Topic
        var deleteTopicsResult = admin.deleteTopics(List.of(topic));
        deleteTopicsResult.all().get();

        admin.close();

    }
}
