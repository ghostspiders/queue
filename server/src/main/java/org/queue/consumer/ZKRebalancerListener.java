package org.queue.consumer;

/**
 * @author gaoyvfeng
 * @ClassName ZKRebalancerListener
 * @description:
 * @datetime 2024年 05月 24日 10:10
 * @version: 1.0
 */
import java.util.*;
import java.util.concurrent.BlockingQueue;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.queue.utils.ZKGroupTopicDirs;
import org.queue.utils.ZkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKRebalancerListener implements IZkChildListener {
    // 日志记录器
    private static final Logger logger = LoggerFactory.getLogger(ZKRebalancerListener.class);
    // 消费者分组名称
    private  String group;
    // 消费者ID
    private  String consumerIdString;
    // ZooKeeper客户端
    private  ZkClient zkClient;
    // 旧的每个主题的分区列表映射
    private Map<String, List<String>> oldPartitionsPerTopicMap;
    // 旧的每个主题的消费者列表映射
    private Map<String, List<String>> oldConsumersPerTopicMap;
    private ZKGroupDirs dirs;
    // 构造函数
    public ZKRebalancerListener(String group, String consumerIdString, ZkClient zkClient) {
        this.group = group;
        dirs = new ZKGroupDirs(group);
        this.consumerIdString = consumerIdString;
        this.zkClient = zkClient;
        this.oldPartitionsPerTopicMap = new HashMap<>();
        this.oldConsumersPerTopicMap = new HashMap<>();
    }

    // 处理子节点变化的回调方法
    @Override
    public void handleChildChange(String parentPath, List<String> curChilds) throws Exception {
        syncedRebalance();
    }
    // 同步重新平衡的方法
    public void syncedRebalance() {
        // 使用synchronized块确保线程安全
        synchronized (rebalanceLock) {
            for (int i = 0; i < connector.MAX_N_RETRIES; i++) {
                logger.info("begin rebalancing consumer " + consumerIdString + " try #" + i);
                boolean done = false;
                try {
                    // 尝试执行rebalance方法
                    done = rebalance();
                } catch (Throwable e) {
                    // 捕获到异常时记录错误日志
                    // 这可能是由于ZK状态在我们迭代时发生变化
                    // 例如，一个ZK节点可能在我们获取所有子节点和获取子节点的值之间消失了
                    // 由于将触发另一次重新平衡，所以这里不做处理
                    logger.error("exception during rebalance", e);
                }
                logger.info("end rebalancing consumer " + consumerIdString + " try #" + i);
                if (done) {
                    // 如果完成重新平衡，则返回
                    return;
                }
                // 如果没有完成，则释放所有分区，重置状态并重试
                releasePartitionOwnership();
                resetState();
                try {
                    // 根据配置的同步时间等待
                    Thread.sleep(config.zkSyncTimeMs);
                } catch (InterruptedException e) {
                    // 线程中断异常处理
                    Thread.currentThread().interrupt();
                }
            }
        }

        // 如果经过最大重试次数后仍无法重新平衡，抛出运行时异常
        throw new RuntimeException(consumerIdString + " can't rebalance after " + connector.MAX_N_RETRIES + " retries");
    }
    // 释放分区所有权
    private void releasePartitionOwnership() {
        // 假设topicRegistry是一个Map<String, Map<String, Set<String>>>类型的实例
        for (Map.Entry<String, Map<String, Set<String>>> entry : topicRegistry.entrySet()) {
            String topic = entry.getKey();
            Map<String, Set<String>> infos = entry.getValue();
            ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(group, topic);
            for (String partition : infos.keySet()) {
                String znode = topicDirs.consumerOwnerDir + "/" + partition;
                ZkUtils.deletePath(zkClient, znode);
                if (logger.isDebugEnabled()) {
                    logger.debug("Consumer " + consumerIdString + " releasing " + znode);
                }
            }
        }
    }

    // 获取每个主题的消费者列表
    private Map<String, List<String>> getConsumersPerTopic(String group) {
        List<String> consumers = ZkUtils.getChildrenParentMayNotExist(zkClient, dirs.consumerRegistryDir);
        Map<String, List<String>> consumersPerTopicMap = new HashMap<>();
        for (String consumer : consumers) {
            TopicCount topicCount = getTopicCount(consumer);
            Map<String, Set<String>> consumerThreadIdsPerTopic = topicCount.getConsumerThreadIdsPerTopic();
            for (Map.Entry<String, Set<String>> entry : consumerThreadIdsPerTopic.entrySet()) {
                String topic = entry.getKey();
                Set<String> consumerThreadIdSet = entry.getValue();
                consumersPerTopicMap.computeIfAbsent(topic, k -> new ArrayList<>()).addAll(consumerThreadIdSet);
            }
        }
        for (Map.Entry<String, List<String>> entry : consumersPerTopicMap.entrySet()) {
            List<String> sortedConsumerList = new ArrayList<>(entry.getValue());
            Collections.sort(sortedConsumerList);
            entry.setValue(sortedConsumerList);
        }
        return consumersPerTopicMap;
    }

    // 获取相关主题的线程ID映射
    private Map<String, Set<String>> getRelevantTopicMap(Map<String, Set<String>> myTopicThreadIdsMap,
                                                         Map<String, List<String>> newPartMap,
                                                         Map<String, List<String>> oldPartMap,
                                                         Map<String, List<String>> newConsumerMap,
                                                         Map<String, List<String>> oldConsumerMap) {
        Map<String, Set<String>> relevantTopicThreadIdsMap = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : myTopicThreadIdsMap.entrySet()) {
            String topic = entry.getKey();
            Set<String> consumerThreadIdSet = entry.getValue();
            if (!Objects.equals(oldPartMap.get(topic), newPartMap.get(topic)) ||
                    !Objects.equals(oldConsumerMap.get(topic), newConsumerMap.get(topic))) {
                relevantTopicThreadIdsMap.put(topic, consumerThreadIdSet);
            }
        }
        return relevantTopicThreadIdsMap;
    }

    // 根据消费者ID获取TopicCount对象
    private TopicCount getTopicCount(String consumerId) {
        String topicCountJson = ZkUtils.readDataMaybeNull(zkClient, dirs.consumerRegistryDir + "/" + consumerId);
        return TopicCount.constructTopicCount(consumerId, topicCountJson);
    }

    // 重置状态
    public void resetState() {
        topicRegistry.clear();
        oldConsumersPerTopicMap.clear();
        oldPartitionsPerTopicMap.clear();
    }
    // 执行重新平衡的方法
    private boolean rebalance() {
        // 测试代码
        // if ("group1_consumer1".equals(consumerIdString)) {
        //     logger.info("sleeping " + consumerIdString);
        //     Thread.sleep(20);
        // }
        TopicCount count = getTopicCount(consumerIdString);
        Map<String, Set<String>> myTopicThreadIdsMap = count.getConsumerThreadIdsPerTopic();
        cluster = zkUtils.getCluster(zkClient);
        Map<String, List<String>> consumersPerTopicMap = getConsumersPerTopic(group);
        Map<String, List<String>> partitionsPerTopicMap = zkUtils.getPartitionsForTopics(zkClient, new HashSet<>(myTopicThreadIdsMap.keySet()));
        Map<String, Set<String>> relevantTopicThreadIdsMap = getRelevantTopicMap(
                myTopicThreadIdsMap, partitionsPerTopicMap, oldPartitionsPerTopicMap, consumersPerTopicMap, oldConsumersPerTopicMap);
        if (relevantTopicThreadIdsMap.isEmpty()) {
            logger.info("Consumer " + consumerIdString + " with " + consumersPerTopicMap + " doesn't need to rebalance.");
            return true;
        }

        logger.info("Committing all offsets");
        commitOffsets();

        logger.info("Releasing partition ownership");
        releasePartitionOwnership();

        Set<BlockingQueue<FetchedDataChunk>> queuesToBeCleared = new HashSet<>();
        for (Map.Entry<String, Set<String>> entry : relevantTopicThreadIdsMap.entrySet()) {
            String topic = entry.getKey();
            Set<String> consumerThreadIdSet = entry.getValue();
            topicRegistry.remove(topic);
            topicRegistry.put(topic, new Pool<Partition, PartitionTopicInfo>());

            ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(group, topic);
            List<String> curConsumers = consumersPerTopicMap.get(topic);
            List<String> curPartitions = partitionsPerTopicMap.get(topic);

            int nPartsPerConsumer = curPartitions.size() / curConsumers.size();
            int nConsumersWithExtraPart = curPartitions.size() % curConsumers.size();

            logger.info("Consumer " + consumerIdString + " rebalancing the following partitions: " + curPartitions +
                    " for topic " + topic + " with consumers: " + curConsumers);

            for (String consumerThreadId : consumerThreadIdSet) {
                int myConsumerPosition = curConsumers.indexOf(consumerThreadId);
                assert (myConsumerPosition >= 0);
                int startPart = nPartsPerConsumer * myConsumerPosition + Math.min(myConsumerPosition, nConsumersWithExtraPart);
                int nParts = nPartsPerConsumer + (myConsumerPosition + 1 > nConsumersWithExtraPart ? 0 : 1);

                if (nParts <= 0) {
                    logger.warn("No broker partitions consumed by consumer thread " + consumerThreadId + " for topic " + topic);
                } else {
                    for (int i = startPart; i < startPart + nParts; i++) {
                        String partition = curPartitions.get(i);
                        logger.info(consumerThreadId + " attempting to claim partition " + partition);
                        if (!processPartition(topicDirs, partition, topic, consumerThreadId)) {
                            return false;
                        }
                    }
                    queuesToBeCleared.add(queues.get(new AbstractMap.SimpleEntry<>(topic, consumerThreadId)));
                }
            }
        }
        updateFetcher(cluster, queuesToBeCleared);
        oldPartitionsPerTopicMap = partitionsPerTopicMap;
        oldConsumersPerTopicMap = consumersPerTopicMap;
        return true;
    }
    // 更新获取器的方法
    private void updateFetcher(Cluster cluster, Iterable<BlockingQueue<FetchedDataChunk>> queuesToBeCleared) {
        List<PartitionTopicInfo> allPartitionInfos = new ArrayList<>();
        for (Pool<Partition, PartitionTopicInfo> partitionInfos : topicRegistry.values()) {
            for (PartitionTopicInfo partition : partitionInfos.values()) {
                allPartitionInfos.add(partition);
            }
        }
        // 根据分区号对分区信息进行排序
        Collections.sort(allPartitionInfos, (s, t) -> s.getPartition().compareTo(t.getPartition()));
        logger.info("Consumer " + consumerIdString + " selected partitions: " +
                allPartitionInfos.stream().map(Object::toString).collect(Collectors.joining(",")));

        // 如果存在获取器，则初始化连接
        if (fetcher != null) {
            fetcher.initConnections(allPartitionInfos, cluster, queuesToBeCleared);
        }
    }

    // 处理分区的方法
    private boolean processPartition(ZKGroupTopicDirs topicDirs, String partition,
                                     String topic, String consumerThreadId) {
        String partitionOwnerPath = topicDirs.consumerOwnerDir + "/" + partition;
        try {
            zkUtils.createEphemeralPathExpectConflict(zkClient, partitionOwnerPath, consumerThreadId);
        } catch (ZkNodeExistsException e) {
            // 如果节点未被原所有者删除，则等待并重试
            logger.info("waiting for the partition ownership to be deleted: " + partition);
            return false;
        } catch (Throwable e2) {
            throw e2;
        }
        addPartitionTopicInfo(topicDirs, partition, topic, consumerThreadId);
        return true;
    }

    // 向注册表添加分区主题信息的方法
    private void addPartitionTopicInfo(ZKGroupTopicDirs topicDirs, String partitionString,
                                       String topic, String consumerThreadId) {
        Partition partition = Partition.parse(partitionString); // 假设存在Partition类和parse方法
        Map<Partition, PartitionTopicInfo> partTopicInfoMap = topicRegistry.get(topic);

        String znode = topicDirs.consumerOffsetDir + "/" + partition.getName();
        String offsetString = zkUtils.readDataMaybeNull(zkClient, znode);
        long offset = (offsetString == null) ? Long.MAX_VALUE : Long.parseLong(offsetString);
        BlockingQueue<FetchedDataChunk> queue = queues.get(new AbstractMap.SimpleEntry<>(topic, consumerThreadId));
        AtomicLong consumedOffset = new AtomicLong(offset);
        AtomicLong fetchedOffset = new AtomicLong(offset);
        PartitionTopicInfo partTopicInfo = new PartitionTopicInfo(
                topic,
                partition.getBrokerId(),
                partition,
                queue,
                consumedOffset,
                fetchedOffset,
                new AtomicInteger(config.fetchSize)
        );
        partTopicInfoMap.put(partition, partTopicInfo);
        if (logger.isDebugEnabled()) {
            logger.debug(partTopicInfo + " selected new offset " + offset);
        }
    }
}