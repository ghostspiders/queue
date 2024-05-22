package org.queue.utils;

import java.util.Properties;
import org.queue.utils.Utils;

/**
 * Zookeeper配置类。
 */
public class ZKConfig {
    /** Zookeeper主机字符串 */
    private String zkConnect;

    /** Zookeeper会话超时时间 */
    private long zkSessionTimeoutMs;

    /** 客户端等待建立Zookeeper连接的最大时间 */
    private long zkConnectionTimeoutMs;

    /** ZK跟随者可以落后ZK领导者的最大时间 */
    private long zkSyncTimeMs;

    /**
     * 构造函数，初始化Zookeeper配置。
     * @param props 包含Zookeeper配置的Properties对象
     */
    public ZKConfig(Properties props) {
        // 初始化ZK连接字符串
        this.zkConnect = Utils.getString(props, "zk.connect", null);

        // 初始化Zookeeper会话超时时间，默认为6000秒
        this.zkSessionTimeoutMs = Utils.getInt(props, "zk.sessiontimeout.ms", 6000 * 1000);

        // 初始化Zookeeper连接超时时间，默认与会话超时时间相同
        this.zkConnectionTimeoutMs = Utils.getInt(props, "zk.connectiontimeout.ms", this.zkSessionTimeoutMs);

        // 初始化Zookeeper同步时间，默认为2000毫秒
        this.zkSyncTimeMs = Utils.getInt(props, "zk.synctime.ms", 2000);
    }

    // 提供getter方法来访问配置参数
    public String getZkConnect() {
        return zkConnect;
    }

    public long getZkSessionTimeoutMs() {
        return zkSessionTimeoutMs;
    }

    public long getZkConnectionTimeoutMs() {
        return zkConnectionTimeoutMs;
    }

    public long getZkSyncTimeMs() {
        return zkSyncTimeMs;
    }
}