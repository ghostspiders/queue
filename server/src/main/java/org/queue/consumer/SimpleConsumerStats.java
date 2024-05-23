package org.queue.consumer;

public class SimpleConsumerStats implements SimpleConsumerStatsMBean {
    private SnapshotStats fetchRequestStats = new SnapshotStats(); // 假设SnapshotStats类已在Java中定义

    public void recordFetchRequest(long requestNs) {
        fetchRequestStats.recordRequestMetric(requestNs);
    }

    public void recordConsumptionThroughput(long data) {
        fetchRequestStats.recordThroughputMetric(data);
    }

    @Override
    public double getFetchRequestsPerSecond() {
        return fetchRequestStats.getRequestsPerSecond();
    }

    @Override
    public double getAvgFetchRequestMs() {
        return fetchRequestStats.getAvgMetric() / (1000.0 * 1000.0);
    }

    @Override
    public double getMaxFetchRequestMs() {
        return fetchRequestStats.getMaxMetric() / (1000.0 * 1000.0);
    }

    @Override
    public long getNumFetchRequests() {
        return fetchRequestStats.getNumRequests();
    }

    @Override
    public double getConsumerThroughput() {
        return fetchRequestStats.getThroughput();
    }
}

// 静态内部类用于处理日志和MBean注册
public class SimpleConsumerStatsManager {
    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumerStatsManager.class);
    private static final String simpleConsumerStatsMBeanName = "queue:type=queue.SimpleConsumerStats";
    private static SimpleConsumerStats stats = new SimpleConsumerStats();

    static {
        try {
            // 注册MBean，这里需要使用Java的JMX（Java Management Extensions）API
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName objectName = new ObjectName(simpleConsumerStatsMBeanName);
            mbs.registerMBean(stats, objectName);
        } catch (Exception e) {
            logger.error("Error registering MBean", e);
        }
    }

    public static void recordFetchRequest(long requestMs) {
        stats.recordFetchRequest(requestMs);
    }

    public static void recordConsumptionThroughput(long data) {
        stats.recordConsumptionThroughput(data);
    }
}