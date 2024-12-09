package store.xianglin.kafka.producer;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class CustomerSerializer implements Serializer<Customer> {
    @Override
    public byte[] serialize(String topic, Customer data) {
        if (data == null) {
            return null;
        }
        var customerName = data.getCustomerName();
        var serializedName = new byte[0];
        if (customerName != null) {
            serializedName = customerName.getBytes(StandardCharsets.UTF_8);
        }
        // 序列化规则：4 字节存 ID； 4 字节存 名称 长度；最后存名称
        var buffer = ByteBuffer.allocate(4 + 4 + serializedName.length);
        buffer.putInt(data.getCustomerID());
        buffer.putInt(serializedName.length);
        buffer.put(serializedName);
        return buffer.array();
    }
}
