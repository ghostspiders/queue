package org.queue.consumer;

/**
 * @author gaoyvfeng
 * @ClassName FetchedDataChunk
 * @description:
 * @datetime 2024年 05月 22日 18:06
 * @version: 1.0
 */
import org.queue.message.ByteBufferMessageSet;

/**
 * 表示从Kafka代理获取的数据块的类。
 */
public class FetchedDataChunk {
    // 假设ByteBufferMessageSet是一个包含ByteBuffer消息集合的类
    private ByteBufferMessageSet messages;
    // 假设PartitionTopicInfo是一个包含分区主题信息的类
    private PartitionTopicInfo topicInfo;
    private long fetchOffset; // 此次获取操作的偏移量

    /**
     * 创建一个新的FetchedDataChunk实例。
     *
     * @param messages 包含获取到的消息的ByteBufferMessageSet对象。
     * @param topicInfo 分区主题信息的PartitionTopicInfo对象。
     * @param fetchOffset 此次获取操作的偏移量。
     */
    public FetchedDataChunk(ByteBufferMessageSet messages, PartitionTopicInfo topicInfo, long fetchOffset) {
        this.messages = messages;
        this.topicInfo = topicInfo;
        this.fetchOffset = fetchOffset;
    }

    // 获取消息集合的getter方法
    public ByteBufferMessageSet getMessages() {
        return messages;
    }

    // 设置消息集合的setter方法
    public void setMessages(ByteBufferMessageSet messages) {
        this.messages = messages;
    }

    // 获取分区主题信息的getter方法
    public PartitionTopicInfo getTopicInfo() {
        return topicInfo;
    }

    // 设置分区主题信息的setter方法
    public void setTopicInfo(PartitionTopicInfo topicInfo) {
        this.topicInfo = topicInfo;
    }

    // 获取此次获取操作的偏移量的getter方法
    public long getFetchOffset() {
        return fetchOffset;
    }

    // 设置此次获取操作的偏移量的setter方法
    public void setFetchOffset(long fetchOffset) {
        this.fetchOffset = fetchOffset;
    }

}
