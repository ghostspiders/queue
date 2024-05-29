package org.queue.producer;

/**
 * @author gaoyvfeng
 * @ClassName SyncProducerStats
 * @description:
 * @datetime 2024年 05月 24日 17:31
 * @version: 1.0
 */
import org.queue.utils.SnapshotStats;
import org.queue.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * 同步生产者统计信息实现。
 */
public class SyncProducerStats implements SyncProducerStatsMBean {
    private SnapshotStats produceRequestStats = new SnapshotStats(600);
    private static final String queueProducerStatsMBeanName = "queue:type=queue.queueProducerStats";
    private static final SyncProducerStats stats = new SyncProducerStats();
    private static final Logger logger = LoggerFactory.getLogger(SyncProducerStats.class);
    static {
        // 注册JMX管理Bean，以下方法需要根据实际情况实现
        Utils.registerMBean(stats, queueProducerStatsMBeanName);
    }

    /**
     * 记录单次发送请求耗时（毫秒）。
     * @param requestMs 请求耗时，单位为毫秒
     */
    public static void recordProduceRequest(long requestMs) {
        stats.recordProduceRequest(requestMs * 1000 * 1000); // 将毫秒转换为纳秒
    }
    @Override
    public double getProduceRequestsPerSecond() {
        return produceRequestStats.getRequestsPerSecond();
    }

    @Override
    public double getAvgProduceRequestMs() {
        // 假设SnapshotStats的getAvgMetric返回的是纳秒，因此需要转换为毫秒
        return produceRequestStats.getAvgMetric() / (1000.0 * 1000.0);
    }

    @Override
    public double getMaxProduceRequestMs() {
        // 同上，假设单位是纳秒
        return produceRequestStats.getMaxMetric() / (1000.0 * 1000.0);
    }

    @Override
    public long getNumProduceRequests() {
        return produceRequestStats.getNumRequests();
    }

}