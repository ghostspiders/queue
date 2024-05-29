package org.queue.javaapi;

import org.queue.api.RequestKeys;
import org.queue.javaapi.message.ByteBufferMessageSet;
import org.queue.network.Request;

import java.nio.ByteBuffer;
import java.util.Objects;

public class ProducerRequest extends Request {
    private final String topic; // 主题
    private final int partition; // 分区
    private final ByteBufferMessageSet messages; // 消息集合

    // 接受器请求的底层实现
    private final org.queue.api.ProducerRequest underlying;

    public ProducerRequest(String topic, int partition, ByteBufferMessageSet messages) {
        super(RequestKeys.produce); // 调用父类构造函数，设置请求类型
        this.topic = topic;
        this.partition = partition;
        this.messages = messages;
        this.underlying = new org.queue.api.ProducerRequest(topic, partition, messages);
    }

    public void writeTo(ByteBuffer buffer) {
        underlying.writeTo(buffer); // 将请求写入ByteBuffer
    }

    public int sizeInBytes() {
        return underlying.sizeInBytes(); // 获取请求大小（字节）
    }

    public int getTranslatedPartition(java.util.function.Function<String, Integer> randomSelector) {
        return underlying.getTranslatedPartition(randomSelector); // 获取转换后的分区
    }

    @Override
    public String toString() {
        return underlying.toString(); // 返回请求的字符串表示
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        ProducerRequest that = (ProducerRequest) other;
        return topic.equals(that.topic) && partition == that.partition &&
                messages.equals(that.messages);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, messages);
    }
}