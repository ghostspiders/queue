package org.queue.producer;

import org.queue.common.InvalidConfigException;
import org.queue.message.CompressionCodec;
import org.queue.utils.Utils;
import org.queue.utils.ZKConfig;

import java.util.List;
import java.util.Properties;

public class ProducerConfig extends ZKConfig {
    private  Properties props;
    public ProducerConfig(Properties props) {
        super(props);
        this.props = props;
    }

    public Properties getProps() {
        return props;
    }

    public String getHost() {
        return Utils.getString(getProps(), "host","127.0.0.1");
    }

    public int getPort() {
        return Utils.getInt(getProps(), "port", 9200);
    }

    public int getBufferSize() {
        return Utils.getInt(getProps(), "buffer.size", 100 * 1024);
    }

    public int getConnectTimeoutMs() {
        return Utils.getInt(getProps(), "connect.timeout.ms", 5000);
    }

    public int getSocketTimeoutMs() {
        return Utils.getInt(getProps(), "socket.timeout.ms", 30000);
    }

    public int getReconnectInterval() {
        return Utils.getInt(getProps(), "reconnect.interval", 30000);
    }

    public int getMaxMessageSize() {
        return Utils.getInt(getProps(), "max.message.size", 1000000);
    }
    public int getQueueTime() {
        return Utils.getInt(getProps(), "queue.time", 5000);
    }

    // 默认方法，用于获取queueSize的值
    public int getQueueSize() {
        return Utils.getInt(getProps(), "queue.size", 10000);
    }

    // 默认方法，用于获取batchSize的值
    public int getBatchSize() {
        return Utils.getInt(getProps(), "batch.size", 200);
    }

    // 默认方法，用于获取serializerClass的值
    public String getSerializerClass() {
        return Utils.getString(getProps(), "serializer.class", "org.queue.serializer.DefaultDecoder");
    }

    // 默认方法，用于获取cbkHandler的值
    public String getCbkHandler() {
        return Utils.getString(getProps(), "callback.handler", null);
    }

    // 默认方法，用于获取cbkHandlerProps的值
    public Properties getCbkHandlerProps() {
        return Utils.getProps(getProps(), "callback.handler.props", null);
    }

    // 默认方法，用于获取eventHandler的值
    public String getEventHandler() {
        return Utils.getString(getProps(), "event.handler", null);
    }

    // 默认方法，用于获取eventHandlerProps的值
    public Properties getEventHandlerProps() {
        return Utils.getProps(getProps(), "event.handler.props", null);
    }
    public String getBrokerPartitionInfo() {
        final String brokerPartitionInfo = Utils.getString(getProps(), "broker.list", null);
        // 检查如果 "broker.list" 和 "partitioner.class" 都不为 null，则抛出异常
        if (brokerPartitionInfo != null && Utils.getString(getProps(), "partitioner.class", null) != null) {
            throw new InvalidConfigException("partitioner.class cannot be used when broker.list is set");
        }
        return brokerPartitionInfo;
    }
    public String getPartitionerClass() {
        return Utils.getString(getProps(), "partitioner.class", "org.queue.producer.DefaultPartitioner");
    }
    public String getProducerType() {
        // 获取生产者类型，默认为 "sync"（同步发送）
        return Utils.getString(getProps(), "producer.type", "sync");
    }
    // 获取压缩编解码器，默认为 NoCompressionCodec
    public CompressionCodec getCompressionCodec() {
        return  Utils.getCompressionCodec(getProps(), "compression.codec");
    }

    // 获取压缩主题列表，如果没有设置则返回空列表
    public List<String> getCompressedTopics() {
        return  Utils.getCSVList(Utils.getString(getProps(), "compressed.topics", null));
    }
}
