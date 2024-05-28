package org.queue.producer;

import org.queue.utils.Utils;
import org.queue.utils.ZKConfig;

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

    public int bufferSize() {
        return Utils.getInt(getProps(), "buffer.size", 100 * 1024);
    }

    public int connectTimeoutMs() {
        return Utils.getInt(getProps(), "connect.timeout.ms", 5000);
    }

    public int socketTimeoutMs() {
        return Utils.getInt(getProps(), "socket.timeout.ms", 30000);
    }

    public int reconnectInterval() {
        return Utils.getInt(getProps(), "reconnect.interval", 30000);
    }

    public int maxMessageSize() {
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






}
