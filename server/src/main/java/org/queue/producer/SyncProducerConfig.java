package org.queue.producer;

import java.util.Properties;

public class SyncProducerConfig {
    private  Properties props;

    /**
     * 构造函数，使用Properties对象来初始化配置。
     *
     * @param props 包含配置信息的Properties对象
     */
    public SyncProducerConfig(Properties props) {
        this.props = props;
    }

    // 提供getter方法来访问属性
    public String getHost() {
        return Utils.getString(props, "host","127.0.0.1");
    }

    public int getPort() {
        return Utils.getString(props, "port", "9200");
    }
    // 默认方法代替Scala中的val，用于获取bufferSize的值
    public int bufferSize() {
        return Utils.getInt(getProps(), "buffer.size", 100 * 1024);
    }

    public int connectTimeoutMs() {
        return Utils.getInt(getProps(), "connect.timeout.ms", 5000);
    }

    // socketTimeoutMs的默认实现
    public int socketTimeoutMs() {
        return Utils.getInt(getProps(), "socket.timeout.ms", 30000);
    }

    public int reconnectInterval() {
        return Utils.getInt(getProps(), "reconnect.interval", 30000);
    }

    public int maxMessageSize() {
        return Utils.getInt(getProps(), "max.message.size", 1000000);
    }
}