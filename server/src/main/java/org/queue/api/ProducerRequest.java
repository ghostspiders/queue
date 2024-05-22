package org.queue.api;

/**
 * @author gaoyvfeng
 * @ClassName ProducerRequest
 * @description:
 * @datetime 2024年 05月 22日 10:27
 * @version: 1.0
 */
import org.queue.message.ByteBufferMessageSet;
import org.queue.network.Request;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * 消息生产者请求类，继承自Request基类。
 */
public class ProducerRequest extends Request {
    /**
     * 随机分区的常量值。
     */
    public static final int RandomPartition = -1;

    // 成员变量
    private String topic;      // 主题
    private int partition;    // 分区
    private ByteBufferMessageSet messages;  // 消息集合

    /**
     * 构造函数。
     * @param topic 主题名称
     * @param partition 分区编号
     * @param messages 消息集合
     */
    public ProducerRequest(String topic, int partition, ByteBufferMessageSet messages) {
        super(RequestKeys.produce); // 调用基类的构造函数
        this.topic = topic;
        this.partition = partition;
        this.messages = messages;
    }

    /**
     * 从ByteBuffer中读取数据并创建ProducerRequest对象。
     * @param buffer ByteBuffer对象
     * @return 生成的ProducerRequest对象
     */
    public  ProducerRequest readFrom(ByteBuffer buffer) {
        String topic = Utils.readShortString(buffer, StandardCharsets.UTF_8);
        int partition = buffer.getInt();
        int messageSetSize = buffer.getInt();
        ByteBuffer messageSetBuffer = buffer.slice();
        messageSetBuffer.limit(messageSetSize);
        buffer.position(buffer.position() + messageSetSize);
        return new ProducerRequest(topic, partition, new ByteBufferMessageSet(messageSetBuffer));
    }

    /**
     * 将ProducerRequest对象的数据写入ByteBuffer中。
     * @param buffer ByteBuffer对象
     */
    @Override
    public void writeTo(ByteBuffer buffer) {
        Utils.writeShortString(buffer, topic, StandardCharsets.UTF_8);
        buffer.putInt(partition);
        ByteBuffer serialized = messages.serialized();
        buffer.putInt(serialized.remaining());
        buffer.put(serialized);
        serialized.rewind();
    }

    /**
     * 计算ProducerRequest对象的字节大小。
     * @return 请求的字节大小
     */
    @Override
    public int sizeInBytes() {
        int size = 2 + topic.length() + 4 + 4 + messages.sizeInBytes();
        return size;
    }

    /**
     * 获取翻译后的分区编号，如果是随机分区则使用选择器函数。
     * @param randomSelector 分区选择器函数
     * @return 分区编号
     */
    public int getTranslatedPartition(java.util.function.Function<String, Integer> randomSelector) {
        if (partition == RandomPartition) {
            return randomSelector.apply(topic);
        } else {
            return partition;
        }
    }

    /**
     * 返回ProducerRequest对象的字符串表示。
     * @return 字符串表示
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ProducerRequest(")
                .append(topic).append(",")
                .append(partition).append(",")
                .append(messages.sizeInBytes())
                .append(")");
        return builder.toString();
    }

    /**
     * 重写equals方法，用于对象比较。
     * @param other 要比较的其他对象
     * @return 是否相等
     */
    @Override
    public boolean equals(Object other) {
        if (other instanceof ProducerRequest) {
            ProducerRequest that = (ProducerRequest) other;
            return topic == that.topic && partition == that.partition &&
                    messages.equals(that.messages);
        }else {
            return false;
        }
    }

    /**
     * 重写hashCode方法，用于哈希表存储。
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return 31 + (17 * partition) + Objects.hash(topic, messages);
    }
}