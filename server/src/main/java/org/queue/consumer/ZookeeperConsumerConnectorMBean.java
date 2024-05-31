package org.queue.consumer;

/**
 * @author gaoyvfeng
 * @ClassName ZookeeperConsumerConnectorMBean
 * @description:
 * @datetime 2024年 05月 24日 10:40
 * @version: 1.0
 */
public interface ZookeeperConsumerConnectorMBean {
    /**
     * 获取分区所有者统计信息。
     * @return 分区所有者统计信息的字符串表示。
     */
    String getPartOwnerStats();

    /**
     * 获取消费者分组名称。
     * @return 消费者分组名称。
     */
    String getConsumerGroup();

    /**
     * 获取指定主题、代理ID和分区ID的偏移量滞后情况。
     * @param topic       主题名称
     * @param brokerId    代理ID
     * @param partitionId  分区ID
     * @return 偏移量滞后数，即尚未消费的消息数量。
     */
    long getOffsetLag(String topic, int brokerId, int partitionId);

    /**
     * 获取指定主题、代理ID和分区ID的消费偏移量。
     * @param topic       主题名称
     * @param brokerId    代理ID
     * @param partitionId  分区ID
     * @return 当前消费的偏移量。
     */
    long getConsumedOffset(String topic, int brokerId, int partitionId);

    /**
     * 获取指定主题、代理ID和分区ID的最新偏移量。
     * @param topic       主题名称
     * @param brokerId    代理ID
     * @param partitionId  分区ID
     * @return 最新偏移量，即已发布的消息总数。
     */
    long getLatestOffset(String topic, int brokerId, int partitionId);
}