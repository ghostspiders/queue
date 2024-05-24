package org.queue.consumer;

/**
 * @author gaoyvfeng
 * @ClassName ConsumerConfig
 * @description:
 * @datetime 2024年 05月 22日 17:03
 * @version: 1.0
 */
import org.apache.zookeeper.common.ZKConfig;
import org.queue.api.OffsetRequest;
import org.queue.utils.Utils;
import java.util.Properties;

public class ConsumerConfig extends ZKConfig {
    // 静态常量，作为默认配置值
    private static final int SocketTimeout = 30 * 1000;
    private static final int SocketBufferSize = 64 * 1024;
    private static final int FetchSize = 300 * 1024;
    private static final int MaxFetchSize = 10 * FetchSize;
    private static final long BackoffIncrementMs = 1000;
    private static final boolean AutoCommit = true;
    private static final int AutoCommitInterval = 10 * 1000;
    private static final int MaxQueuedChunks = 100;
    private static final String AutoOffsetReset = OffsetRequest.SmallestTimeString;
    private static final int ConsumerTimeoutMs = -1;
    private static final String EmbeddedConsumerTopics = "";

    // 实际配置值，从Properties中读取或使用默认值
    private String groupId;
    private String consumerId;
    private int socketTimeoutMs;
    private int socketBufferSize;
    private int fetchSize;
    private int maxFetchSize;
    private long backoffIncrementMs;
    private boolean autoCommit;
    private int autoCommitIntervalMs;
    private int maxQueuedChunks;
    private String autoOffsetReset;
    private int consumerTimeoutMs;
    private Properties embeddedConsumerTopicMap;

    public ConsumerConfig(Properties props) {
        super(props);
        // 使用Utils工具类从Properties中获取配置值，如果没有设置则使用默认值
        this.groupId = Utils.getString(props, "groupid", "");
        this.consumerId = Utils.getString(props, "consumerid", null);
        this.socketTimeoutMs = Utils.getInt(props, "socket.timeout.ms", SocketTimeout);
        this.socketBufferSize = Utils.getInt(props, "socket.buffersize", SocketBufferSize);
        this.fetchSize = Utils.getInt(props, "fetch.size", FetchSize);
        this.maxFetchSize = Utils.getInt(props, "max.fetch.size", MaxFetchSize);
        this.backoffIncrementMs = Utils.getLong(props, "backoff.increment.ms", BackoffIncrementMs);
        this.autoCommit = Utils.getBoolean(props, "autocommit.enable", AutoCommit);
        this.autoCommitIntervalMs = Utils.getInt(props, "autocommit.interval.ms", AutoCommitInterval);
        this.maxQueuedChunks = Utils.getInt(props, "queuedchunks.max", MaxQueuedChunks);
        this.autoOffsetReset = Utils.getString(props, "autooffset.reset", AutoOffsetReset);
        this.consumerTimeoutMs = Utils.getInt(props, "consumer.timeout.ms", ConsumerTimeoutMs);
        this.embeddedConsumerTopicMap = Utils.getProperties(props, "embeddedconsumer.topics", EmbeddedConsumerTopics);
    }

    // Getters for all fields
    public String getGroupId() {
        return groupId;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public int getSocketTimeoutMs() {
        return socketTimeoutMs;
    }

    public int getSocketBufferSize() {
        return socketBufferSize;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public int getMaxFetchSize() {
        return maxFetchSize;
    }

    public long getBackoffIncrementMs() {
        return backoffIncrementMs;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public int getAutoCommitIntervalMs() {
        return autoCommitIntervalMs;
    }

    public int getMaxQueuedChunks() {
        return maxQueuedChunks;
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public int getConsumerTimeoutMs() {
        return consumerTimeoutMs;
    }

    public Properties getEmbeddedConsumerTopicMap() {
        return embeddedConsumerTopicMap;
    }
}
