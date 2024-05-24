package org.queue.producer;

/**
 * @author gaoyvfeng
 * @ClassName SyncProducerStatsMBean
 * @description:
 * @datetime 2024年 05月 24日 17:30
 * @version: 1.0
 */
/**
 * 同步生产者统计信息管理Bean接口。
 */
public interface SyncProducerStatsMBean {
    /**
     * 获取每秒发送请求的数量。
     * @return 每秒发送请求的数量。
     */
    double getProduceRequestsPerSecond();

    /**
     * 获取发送请求平均耗时（毫秒）。
     * @return 发送请求平均耗时（毫秒）。
     */
    double getAvgProduceRequestMs();

    /**
     * 获取发送请求最大耗时（毫秒）。
     * @return 发送请求最大耗时（毫秒）。
     */
    double getMaxProduceRequestMs();

    /**
     * 获取发送请求的总数。
     * @return 发送请求的总数。
     */
    long getNumProduceRequests();
}