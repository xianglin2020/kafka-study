package store.xianglin.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;

/**
 * 分区器
 *
 * @author xianglin
 * @see ProducerConfig.PARTITIONER_CLASS_CONFIG
 */
public class BananaPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (keyBytes == null || !(key instanceof String)) {
            throw new InvalidRecordException("Key must be a string");
        }

        var partitions = cluster.partitionsForTopic(topic);
        var size = partitions.size();
        if (key.equals("Banana")) {
            return size - 1;
        }
        return Math.abs(Utils.murmur2(keyBytes)) % (size - 1);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {


    }
}
