package com.example.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区策略
 *
 * @author xianglin
 */
public class SamplePartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String keyStr = (String) key;
        String prefix = "key-";
        if (keyStr.startsWith(prefix)) {
            Integer partitionCountForTopic = cluster.partitionCountForTopic(topic);
            int keyIndex = Integer.parseInt(keyStr.substring(prefix.length()));
            return keyIndex % partitionCountForTopic;
        }
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}