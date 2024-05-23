package org.queue.server;

/**
 * @author gaoyvfeng
 * @ClassName KafkaConfig
 * @description:
 * @datetime 2024年 05月 23日 15:58
 * @version: 1.0
 */
import org.queue.utils.ZKConfig;

import java.util.Properties;
import java.util.Map;

public class QueueConfig extends ZKConfig {
    // 监听和接受连接的端口
    private int port;
    // 代理的主机名
    private String hostName;
    // 此服务器的代理ID
    private int brokerId;
    // 套接字服务器套接字的SO_SNDBUFF缓冲区大小
    private int socketSendBuffer;
    // 套接字服务器套接字的SO_RCVBUFF缓冲区大小
    private int socketReceiveBuffer;
    // 套接字请求中字节的最大数量
    private int maxSocketRequestSize;
    // 服务器用于处理所有客户端请求的工作线程数
    private int numThreads;
    // 测量性能统计信息的间隔（秒）
    private int monitoringPeriodSecs;
    // 主题的默认日志分区数
    private int numPartitions;
    // 存储日志数据的目录
    private String logDir;
    // 单个日志文件的最大大小（字节）
    private int logFileSize;
    // 日志分区上积累的消息数量，超过此数量后将消息刷新到磁盘
    private int flushInterval;
    // 保留日志文件的小时数，之后删除
    private int logRetentionHours;
    // 特定主题保留日志文件的小时数
    private Map<String, Integer> logRetentionHoursMap;
    // 日志清理器检查是否有任何日志需要删除的频率（分钟）
    private int logCleanupIntervalMinutes;
    // 是否启用Zookeeper注册
    private boolean enableZookeeper;
    // 选定主题的消息在内存中保留的最大时间（毫秒），然后刷新到磁盘
    private Map<String, Integer> flushIntervalMap;
    // 日志刷新器检查是否有任何日志需要刷新到磁盘的频率（毫秒）
    private int flushSchedulerThreadRate;
    // 任何主题的消息在内存中保留的最大时间（毫秒），然后刷新到磁盘
    private int defaultFlushIntervalMs;
    // 选定主题的分区数
    private Map<String, Integer> topicPartitionsMap;

    public QueueConfig(Properties props) {
        super(props);
        this.port = Utils.getInt(props, "port", 6667);
        this.hostName = Utils.getString(props, "hostname", null);
        this.brokerId = Utils.getInt(props, "brokerid");
        this.socketSendBuffer = Utils.getInt(props, "socket.send.buffer", 100 * 1024);
        this.socketReceiveBuffer = Utils.getInt(props, "socket.receive.buffer", 100 * 1024);
        this.maxSocketRequestSize = Utils.getIntInRange(props, "max.socket.request.bytes", 100 * 1024 * 1024, 1, Integer.MAX_VALUE);
        this.numThreads = Utils.getIntInRange(props, "num.threads", Runtime.getRuntime().availableProcessors(), 1, Integer.MAX_VALUE);
        this.monitoringPeriodSecs = Utils.getIntInRange(props, "monitoring.period.secs", 600, 1, Integer.MAX_VALUE);
        this.numPartitions = Utils.getIntInRange(props, "num.partitions", 1, 1, Integer.MAX_VALUE);
        this.logDir = Utils.getString(props, "log.dir");
        this.logFileSize = Utils.getIntInRange(props, "log.file.size", 1 * 1024 * 1024 * 1024, Message.MinHeaderSize, Integer.MAX_VALUE);
        this.flushInterval = Utils.getIntInRange(props, "log.flush.interval", 500, 1, Integer.MAX_VALUE);
        this.logRetentionHours = Utils.getIntInRange(props, "log.retention.hours", 24 * 7, 1, Integer.MAX_VALUE);
        this.logRetentionHoursMap = Utils.getTopicRentionHours(getString(props, "topic.log.retention.hours", ""));
        this.logCleanupIntervalMinutes = Utils.getIntInRange(props, "log.cleanup.interval.mins", 10, 1, Integer.MAX_VALUE);
        this.enableZookeeper = Utils.getBoolean(props, "enable.zookeeper", true);
        this.flushIntervalMap = Utils.getTopicFlushIntervals(getString(props, "topic.flush.intervals.ms", ""));
        this.flushSchedulerThreadRate = Utils.getInt(props, "log.default.flush.scheduler.interval.ms", 3000);
        this.defaultFlushIntervalMs = Utils.getInt(props, "log.default.flush.interval.ms", this.flushSchedulerThreadRate);
        this.topicPartitionsMap = Utils.getTopicPartitions(Utils.getString(props, "topic.partition.count.map", ""));
    }


    public int getPort() {
        return port;
    }

    public String getHostName() {
        return hostName;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public int getSocketSendBuffer() {
        return socketSendBuffer;
    }

    public int getSocketReceiveBuffer() {
        return socketReceiveBuffer;
    }

    public int getMaxSocketRequestSize() {
        return maxSocketRequestSize;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public int getMonitoringPeriodSecs() {
        return monitoringPeriodSecs;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public String getLogDir() {
        return logDir;
    }

    public int getLogFileSize() {
        return logFileSize;
    }

    public int getFlushInterval() {
        return flushInterval;
    }

    public int getLogRetentionHours() {
        return logRetentionHours;
    }

    public Map<String, Integer> getLogRetentionHoursMap() {
        return logRetentionHoursMap;
    }

    public int getLogCleanupIntervalMinutes() {
        return logCleanupIntervalMinutes;
    }

    public boolean isEnableZookeeper() {
        return enableZookeeper;
    }

    public Map<String, Integer> getFlushIntervalMap() {
        return flushIntervalMap;
    }

    public int getFlushSchedulerThreadRate() {
        return flushSchedulerThreadRate;
    }

    public int getDefaultFlushIntervalMs() {
        return defaultFlushIntervalMs;
    }

    public Map<String, Integer> getTopicPartitionsMap() {
        return topicPartitionsMap;
    }
}