package org.queue.message;

/**
 * @author gaoyvfeng
 * @ClassName LogFlushStats
 * @description:
 * @datetime 2024年 05月 23日 11:28
 * @version: 1.0
 */
import org.queue.utils.SnapshotStats;
import org.queue.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * 日志刷新统计类，实现了LogFlushStatsMBean接口。
 */
public class LogFlushStats implements LogFlushStatsMBean {
    private  Logger logger = LoggerFactory.getLogger(LogFlushStats.class);
    private static final String LogFlushStatsMBeanName = "queue:type=queue.LogFlushStats";
    private static final LogFlushStats stats = new LogFlushStats();

    // 静态代码块，用于注册MBean
    static {
        Utils.registerMBean(stats, LogFlushStatsMBeanName);
    }

    /**
     * 记录刷新请求所需的毫秒数。
     *
     * @param requestMs 刷新请求所需的毫秒数。
     */
    public static void recordFlushRequest(long requestMs) {

        stats.recordFlushRequest(requestMs);
    }
    private SnapshotStats flushRequestStats = new SnapshotStats(600); // 刷新请求统计信息

    /**
     * 获取每秒刷新次数。
     *
     * @return 每秒刷新次数的双精度浮点数。
     */
    public double getFlushesPerSecond() {
        return flushRequestStats.getRequestsPerSecond();
    }

    /**
     * 获取平均刷新时间（毫秒）。
     *
     * @return 平均刷新时间的双精度浮点数。
     */
    public double getAvgFlushMs() {
        return flushRequestStats.getAvgMetric();
    }

    /**
     * 获取最大刷新时间（毫秒）。
     *
     * @return 最大刷新时间的双精度浮点数。
     */
    public double getMaxFlushMs() {
        return flushRequestStats.getMaxMetric();
    }

    /**
     * 获取刷新次数。
     *
     * @return 刷新次数的长整型数。
     */
    public long getNumFlushes() {
        return flushRequestStats.getNumRequests();
    }

}