package org.queue.consumer;

/**
 * @author gaoyvfeng
 * @ClassName ZookeeperConsumerConnector
 * @description:
 * @datetime 2024年 05月 24日 10:46
 * @version: 1.0
 */

import org.I0Itec.zkclient.ZkClient;
import org.queue.api.OffsetRequest;
import org.queue.cluster.Broker;
import org.queue.cluster.Cluster;
import org.queue.cluster.Partition;
import org.queue.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ZookeeperConsumerConnector implements ConsumerConnector{
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperConsumerConnector.class);
    // 定义最大重试次数的常量
    public static final int MAX_N_RETRIES = 4;

    // 定义关闭命令的静态实例
    public static final FetchedDataChunk shutdownCommand = new FetchedDataChunk(null, null, -1L);
    private AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private Object rebalanceLock = new Object();
    private Fetcher fetcher; // Assuming Fetcher is a class you have implemented
    private ZkClient zkClient;
    private Pool<String, Pool<Partition, PartitionTopicInfo>> topicRegistry;
    private Pool<Map<String, String>, BlockingQueue<FetchedDataChunk>> queues;
    private QueueScheduler scheduler;
    private boolean enableFetcher;
    private ConsumerConfig config;

    public ZookeeperConsumerConnector(ConsumerConfig config, boolean enableFetcher) {
        this.enableFetcher = enableFetcher;
        this.config = config;
        this.topicRegistry = new Pool();
        this.queues = new Pool();
        this.scheduler = new QueueScheduler(1, "queue-consumer-autocommit-", false);

        initialize();
    }

    public ZookeeperConsumerConnector(ConsumerConfig config) {
        this(config, true);
    }

    private void initialize() {
        connectZk();
        createFetcher();
        if (config.isAutoCommit()) {
            startAutoCommitter();
        }
    }

    private void connectZk() {
        logger.info("Connecting to zookeeper instance at " + config.getZkConnect());
        zkClient = new ZkClient(config.getZkConnect(), config.getZkSessionTimeoutMs(), config.getZkConnectionTimeoutMs());
    }

    private void createFetcher() {
        if (enableFetcher) {
            fetcher = new Fetcher(config, zkClient);
        }
    }

    private void startAutoCommitter() {
        scheduler = new QueueScheduler(1, "queue-consumer-autocommit-", false);
        long autoCommitIntervalMs = config.getAutoCommitIntervalMs();
        logger.info("starting auto committer every " + autoCommitIntervalMs + " ms");
        scheduler.scheduleWithRate(this::autoCommit, autoCommitIntervalMs, autoCommitIntervalMs);
    }

    // 关闭消费者连接器
    public void shutdown() {
        if (isShuttingDown.compareAndSet(false, true)) {
            logger.info("ZKConsumerConnector shutting down");
            try {
                if (scheduler != null) {
                    scheduler.shutdown();
                }
                if (fetcher != null) {
                    fetcher.shutdown();
                }
                sendShutdownToAllQueues(); // 向所有队列发送关闭命令
                if (zkClient != null) {
                    zkClient.close();
                    zkClient = null;
                }
            } catch (Throwable e) {
                logger.error("Error during ZookeeperConsumerConnector shutdown", e);
            }
            logger.info("ZKConsumerConnector shut down completed");
        }
    }

    // 消费消息的方法
    public Map<String, List<QueueMessageStream>> consume(Map<String, Integer> topicCountMap) {
        logger.debug("entering consume");
        if (topicCountMap == null)
            throw new RuntimeException("topicCountMap is null");

        ZKGroupDirs dirs = new ZKGroupDirs(config.getGroupId());
        Map<String, List<QueueMessageStream>> ret = new HashMap<>();

        try {
            String consumerUuid = config.getConsumerId() != null ? config.getConsumerId() : InetAddress.getLocalHost().getHostName() + "-" + System.currentTimeMillis();
            String consumerIdString = config.getGroupId() + "_" + consumerUuid;
            TopicCount topicCount = new TopicCount(consumerIdString, topicCountMap);

            ZKRebalancerListener loadBalancerListener = new ZKRebalancerListener(config.getGroupId(), consumerIdString,zkClient,rebalanceLock,config,topicRegistry,queues,fetcher);
            registerConsumerInZK(dirs, consumerIdString, topicCount);
            zkClient.subscribeChildChanges(dirs.getConsumerRegistryDir(), loadBalancerListener);

            Map<String, Set<String>> consumerThreadIdsPerTopic = topicCount.getConsumerThreadIdsPerTopic();
            for (Map.Entry<String, Set<String>> entry : consumerThreadIdsPerTopic.entrySet()) {
                String topic = entry.getKey();
                Set<String> threadIdSet = entry.getValue();
                List<QueueMessageStream> streamList = new ArrayList<>();
                for (String threadId : threadIdSet) {
                    BlockingQueue<FetchedDataChunk> stream = new LinkedBlockingQueue<>(/* 需要指定容量 */);
                    queues.put(Map.of(topic, threadId), stream);
                    streamList.add(new QueueMessageStream(stream, config.getConsumerTimeoutMs()));
                }
                ret.put(topic, streamList);
                logger.debug("adding topic " + topic + " and stream to map...");
            }

            zkClient.subscribeStateChanges(
                    new ZKSessionExpireListenner(dirs, consumerIdString, topicCount, loadBalancerListener));
            loadBalancerListener.syncedRebalance();

            return ret;
        } catch (UnknownHostException e) {
            throw new RuntimeException("Error generating consumer UUID", e);
        }
    }
    // 在ZooKeeper中注册消费者
    private void registerConsumerInZK(ZKGroupDirs dirs, String consumerIdString, TopicCount topicCount) {
        logger.info("begin registering consumer " + consumerIdString + " in ZK");
        // 创建临时节点并设置其值为topicCount的JSON字符串
        ZkUtils.createEphemeralPathExpectConflict(zkClient, dirs.getConsumerRegistryDir() + "/" + consumerIdString, topicCount.toJsonString());
        logger.info("end registering consumer " + consumerIdString + " in ZK");
    }

    // 向所有队列发送关闭命令
    private void sendShutdownToAllQueues() {
        Iterator<Map.Entry<Map<String, String>, BlockingQueue<FetchedDataChunk>>> iterator = queues.iterator();
        while (iterator.hasNext()){
             Map.Entry<Map<String, String>, BlockingQueue<FetchedDataChunk>> entry = iterator.next();
            BlockingQueue<FetchedDataChunk> queue = entry.getValue();
            logger.debug("Clearing up queue");
            // 清空队列并发送关闭命令
            // 这里假设queue实现了clear方法，实际可能需要根据实际情况实现
            queue.clear();
            queue.add(ZookeeperConsumerConnector.shutdownCommand);
            logger.debug("Cleared queue and sent shutdown command");
        }
    }
    // 自动提交偏移量
    public void autoCommit() {
        if (logger.isTraceEnabled()) {
            logger.trace("auto committing");
        }
        try {
            commitOffsets();
        } catch (Throwable t) {
            // 记录异常并继续
            logger.error("exception during autoCommit: ", t);
        }
    }

    /**
     * 创建消息流。
     *
     * @param topicCountMap 主题到分区数的映射。
     * @return 按主题分组的消息流列表。
     */
    @Override
    public Map<String, List<QueueMessageStream>> createMessageStreams(Map<String, Integer> topicCountMap) {
        return null;
    }

    // 提交偏移量到ZooKeeper
    public void commitOffsets() {
        if (zkClient == null) return;
        Iterator<Map.Entry<String, Pool<Partition, PartitionTopicInfo>>> iterator = topicRegistry.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Pool<Partition, PartitionTopicInfo>> entry = iterator.next();
            String topic = entry.getKey();
            Pool<Partition, PartitionTopicInfo> infos = entry.getValue();
            ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(config.getGroupId(), topic);
            for (PartitionTopicInfo info : infos.values()) {
                long newOffset = info.getConsumeOffset();
                try {
                    ZkUtils.updatePersistentPath(zkClient, topicDirs.getConsumerOffsetDir() + "/" + info.getPartition().getName(),
                            Long.toString(newOffset));
                } catch (Throwable t) {
                    // 记录异常并继续
                    logger.warn("exception during commitOffsets: " + t + Utils.stackTrace(t));
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Committed offset " + newOffset + " for topic " + info);
                }
            }
        }
    }

    // 为JMX获取分区所有者统计信息
    public String getPartOwnerStats() {
        StringBuilder builder = new StringBuilder();
        Iterator<Map.Entry<String, Pool<Partition, PartitionTopicInfo>>> iterator = topicRegistry.iterator();
        while (iterator.hasNext()){
            Map.Entry<String, Pool<Partition, PartitionTopicInfo>> entry = iterator.next();
            String topic = entry.getKey();
            builder.append("\n" + topic + ": [");
            ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(config.getGroupId(), topic);
            for (PartitionTopicInfo partition : entry.getValue().values()) {
                builder.append("\n    {");
                builder.append(partition.getPartition().getName());
                builder.append(",fetch offset:" + partition.getFetchOffset());
                builder.append(",consumer offset:" + partition.getConsumeOffset());
                builder.append("}");
            }
            builder.append("\n        ]");
        }
        return builder.toString();
    }

    // 为JMX获取消费者分组名称
    public String getConsumerGroup() {
        return config.getGroupId();
    }

    // 计算并获取指定主题、代理ID和分区ID的偏移量滞后情况
    public long getOffsetLag(String topic, int brokerId, int partitionId) {
        return getLatestOffset(topic, brokerId, partitionId) - getConsumedOffset(topic, brokerId, partitionId);
    }

    // 获取消费的偏移量
    public long getConsumedOffset(String topic, int brokerId, int partitionId) {
        Partition partition = new Partition(brokerId, partitionId);
        Pool<Partition, PartitionTopicInfo> partitionInfos = topicRegistry.get(topic);
        if (partitionInfos != null) {
            PartitionTopicInfo partitionInfo = partitionInfos.get(partition);
            if (partitionInfo != null) {
                return partitionInfo.getConsumeOffset();
            }
        }
        // 如果没有找到，尝试从ZooKeeper获取
        try {
            ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(config.getGroupId(), topic);
            String znode = topicDirs.getConsumerOffsetDir() + "/" + partition.getName();
            String offsetString = ZkUtils.readDataMaybeNull(zkClient, znode);
            if (offsetString != null) {
                return Long.parseLong(offsetString);
            } else {
                return -1;
            }
        } catch (Throwable e) {
            logger.error("error in getConsumedOffset JMX ", e);
        }
        return -2;
    }

    // 获取最新偏移量
    public long getLatestOffset(String topic, int brokerId, int partitionId) {
        SimpleConsumer simpleConsumer = null;
        long producedOffset = -1L;
        try {
            Cluster cluster = ZkUtils.getCluster(zkClient); // 需要具体实现
            Broker broker = cluster.getBroker(brokerId); // 需要具体实现
            simpleConsumer = new SimpleConsumer(broker.getHost(), broker.getPort(),
                    ConsumerConfig.SocketTimeout, ConsumerConfig.SocketBufferSize);
            long[] latestOffset = simpleConsumer.getOffsetsBefore(topic, partitionId,
                    OffsetRequest.LatestTime, 1);
            producedOffset = latestOffset[0];
        } catch (Throwable e) {
            logger.error("error in getLatestOffset jmx ", e);
        } finally {
            if (simpleConsumer != null) {
                simpleConsumer.close();
            }
        }
        return producedOffset;
    }
}
