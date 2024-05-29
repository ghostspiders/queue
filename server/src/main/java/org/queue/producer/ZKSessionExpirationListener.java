package org.queue.producer;

/**
 * @author gaoyvfeng
 * @ClassName ZKSessionExpirationListener
 * @description:
 * @datetime 2024年 05月 24日 17:52
 * @version: 1.0
 */
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.Watcher;
import org.queue.cluster.Partition;
import org.queue.utils.ZkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.stream.Collectors;

public class ZKSessionExpirationListener implements IZkStateListener {
    private final BrokerTopicsListener brokerTopicsListener;
    private static final Logger logger = LoggerFactory.getLogger(ZKSessionExpirationListener.class);
    private  ZKBrokerPartitionInfo zkBrokerPartitionInfo;
    private  ZkClient zkClient;
    public ZKSessionExpirationListener(BrokerTopicsListener brokerTopicsListener, ZKBrokerPartitionInfo zkBrokerPartitionInfo, ZkClient zkClient) {
        this.brokerTopicsListener = brokerTopicsListener;
        this.zkBrokerPartitionInfo = zkBrokerPartitionInfo;
        this.zkClient = zkClient;
    }

    @Override
    public void handleStateChanged(Watcher.Event.KeeperState keeperState) {

    }

    /**
     * 处理ZooKeeper会话过期后创建了新会话的事件。
     * 在这里，您可能需要重新创建任何临时节点。
     * @throws Exception 出现任何错误时抛出。
     */
    @Override
    public void handleNewSession() {
        /**
         * 当我们收到会话过期事件时，我们失去了所有临时节点，zkclient已经为我们重新建立了连接。
         */
        logger.info("ZK expired; release old list of broker partitions for topics");
        // 获取ZK上的topic-broker分区信息
        zkBrokerPartitionInfo.refresh();
        brokerTopicsListener.resetState();
        // 为每个topic的brokers变化注册监听器以保持topicBrokerPartitions更新
        // 注意：这里可能不需要这样做。因为当我们从getZKTopicPartitionInfo()读取时，
        // 它自动在那里重新创建了监听器本身
        Map<String, SortedSet<Partition>> topicBrokerPartitions = zkBrokerPartitionInfo.getTopicBrokerPartitions();
        List<String> topics = topicBrokerPartitions.keySet().stream().collect(Collectors.toList());
        for (String topic : topics) {
            zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath + "/" + topic, brokerTopicsListener);
        }
        // 没有必要重新注册其他监听器，因为它们监听的是永久节点的子变化
    }

    /**
     * 处理会话建立错误的事件。
     * @param throwable 抛出的异常。
     */
    @Override
    public void handleSessionEstablishmentError(Throwable throwable) {
        // 什么也不做，或者可以记录错误
    }
}
