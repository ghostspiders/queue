package org.queue.producer;

/**
 * @author gaoyvfeng
 * @ClassName BrokerPartitionInfo
 * @description:
 * @datetime 2024年 05月 24日 16:00
 * @version: 1.0
 */

import org.queue.cluster.Broker;
import org.queue.cluster.Partition;

import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;

/**
 * 维护有关Kafka代理分区信息的特质。
 */
public interface BrokerPartitionInfo {

    /**
     * 返回给定主题的代理和分区信息序列。
     * @param topic 主题名称，如果是null，则返回所有主题的信息
     * @return 代理和分区信息的有序集合。如果没有代理可用，则返回空集合。
     */
    SortedSet<Partition> getBrokerPartitionInfo(String topic);

    /**
     * 根据给定的代理ID生成代理的主机和端口信息。
     * @param brokerId 代理ID
     * @return 代理的主机和端口信息的Optional对象，如果没有找到代理，则返回空。
     */
    Optional<Broker> getBrokerInfo(int brokerId);

    /**
     * 为所有代理生成从代理ID到主机和端口的映射。
     * @return 所有代理的ID到主机和端口的映射。
     */
    Map<Integer, Broker> getAllBrokerInfo();

    /**
     * 清理资源。
     */
    void close();
}