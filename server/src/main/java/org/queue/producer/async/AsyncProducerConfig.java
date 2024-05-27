package org.queue.producer.async;

import org.queue.producer.SyncProducerConfig;

import java.util.Properties;

public class AsyncProducerConfig extends SyncProducerConfig {

    private  Properties props;

    public AsyncProducerConfig(Properties props) {
        super(props);
        this.props = props;
    }

    public Properties getProps() {
        return props;
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