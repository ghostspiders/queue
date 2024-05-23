package org.queue.message;

/**
 * @author gaoyvfeng
 * @ClassName LogFlushStatsMBean
 * @description:
 * @datetime 2024年 05月 23日 11:24
 * @version: 1.0
 */
/**
 * LogFlushStatsMBean接口定义了日志刷新统计信息的MBean方法。
 */
public interface LogFlushStatsMBean {
    /**
     * 获取每秒刷新次数。
     * @return 返回每秒刷新次数的双精度浮点数。
     */
    double getFlushesPerSecond();

    /**
     * 获取平均刷新时间（毫秒）。
     * @return 返回平均刷新时间的双精度浮点数。
     */
    double getAvgFlushMs();

    /**
     * 获取最大刷新时间（毫秒）。
     * @return 返回最大刷新时间的双精度浮点数。
     */
    double getMaxFlushMs();

    /**
     * 获取刷新次数。
     * @return 返回刷新次数的长整型数。
     */
    long getNumFlushes();
}
