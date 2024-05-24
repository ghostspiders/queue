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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ZookeeperConsumerConnector extends ZkConsumerConnector implements ZookeeperConsumerConnectorMBean {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperConsumerConnector.class);
    private AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    private Object rebalanceLock = new Object();
    private Fetcher fetcher; // Assuming Fetcher is a class you have implemented
    private ZkClient zkClient;
    private Pool<String, Pool<Partition, PartitionTopicInfo>> topicRegistry;
    private Pool<Tuple2<String, String>, BlockingQueue<FetchedDataChunk>> queues;
    private KafkaScheduler scheduler;
    private boolean enableFetcher;

    public ZookeeperConsumerConnector(ConsumerConfig config, boolean enableFetcher) {
        super(config);
        this.enableFetcher = enableFetcher;
        // Initialize resources and start background tasks
        initialize();
    }

    public ZookeeperConsumerConnector(ConsumerConfig config) {
        this(config, true);
    }

    private void initialize() {
        connectZk();
        createFetcher();
        if (config.autoCommit()) {
            startAutoCommitter();
        }
    }

    private void connectZk() {
        logger.info("Connecting to zookeeper instance at " + config.zkConnect());
        zkClient = new ZkClient(config.zkConnect(), config.zkSessionTimeoutMs(), config.zkConnectionTimeoutMs());
    }

    private void createFetcher() {
        if (enableFetcher) {
            fetcher = new Fetcher(config, zkClient); // You need to implement Fetcher or use an existing class
        }
    }

    private void startAutoCommitter() {
        // You need to implement KafkaScheduler or use an existing class
        scheduler = new KafkaScheduler(1, "queue-consumer-autocommit-", false);
        long autoCommitIntervalMs = config.autoCommitIntervalMs();
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
    public Map<String, List<KafkaMessageStream>> consume(Map<String, Integer> topicCountMap) {
        logger.debug("entering consume");
        if (topicCountMap == null)
            throw new RuntimeException("topicCountMap is null");

        ZKGroupDirs dirs = new ZKGroupDirs(config.groupId());
        Map<String, List<KafkaMessageStream>> ret = new HashMap<>();

        try {
            String consumerUuid = config.consumerId() != null ? config.consumerId() : InetAddress.getLocalHost().getHostName() + "-" + System.currentTimeMillis();
            String consumerIdString = config.groupId() + "_" + consumerUuid;
            TopicCount topicCount = new TopicCount(consumerIdString, topicCountMap);

            ZKRebalancerListener loadBalancerListener = new ZKRebalancerListener(config.groupId(), consumerIdString);
            registerConsumerInZK(dirs, consumerIdString, topicCount);
            zkClient.subscribeChildChanges(dirs.consumerRegistryDir(), loadBalancerListener);

            Map<String, Set<String>> consumerThreadIdsPerTopic = topicCount.getConsumerThreadIdsPerTopic();
            for (Map.Entry<String, Set<String>> entry : consumerThreadIdsPerTopic.entrySet()) {
                String topic = entry.getKey();
                Set<String> threadIdSet = entry.getValue();
                List<KafkaMessageStream> streamList = new ArrayList<>();
                for (String threadId : threadIdSet) {
                    BlockingQueue<FetchedDataChunk> stream = new LinkedBlockingQueue<>(/* 需要指定容量 */);
                    queues.put(new Tuple2<>(topic, threadId), stream);
                    streamList.add(new KafkaMessageStream(stream, config.consumerTimeoutMs()));
                }
                ret.put(topic, streamList);
                logger.debug("adding topic " + topic + " and stream to map...");
            }

            zkClient.subscribeStateChanges(new ZKSessionExpireListener(/* 参数 */));
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
        ZkUtils.createEphemeralPathExpectConflict(zkClient, dirs.consumerRegistryDir() + "/" + consumerIdString, topicCount.toJsonString());
        logger.info("end registering consumer " + consumerIdString + " in ZK");
    }

    // 向所有队列发送关闭命令
    private void sendShutdownToAllQueues() {
        Set<Map.Entry<Tuple2<String, String>, BlockingQueue<FetchedDataChunk>>> entries = queues.entrySet();
        for (Map.Entry<Tuple2<String, String>, BlockingQueue<FetchedDataChunk>> entry : entries) {
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

    // 提交偏移量到ZooKeeper
    public void commitOffsets() {
        if (zkClient == null) return;
        for (Map.Entry<String, Pool<Partition, PartitionTopicInfo>> entry : topicRegistry.entrySet()) {
            String topic = entry.getKey();
            Pool<Partition, PartitionTopicInfo> infos = entry.getValue();
            ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(config.groupId(), topic);
            for (PartitionTopicInfo info : infos.values()) {
                long newOffset = info.getConsumeOffset();
                try {
                    ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir() + "/" + info.partition().name(),
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
        for (Map.Entry<String, Pool<Partition, PartitionTopicInfo>> entry : topicRegistry.entrySet()) {
            String topic = entry.getKey();
            builder.append("\n" + topic + ": [");
            ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(config.groupId(), topic);
            for (PartitionTopicInfo partition : entry.getValue().values()) {
                builder.append("\n    {");
                builder.append(partition.partition().name());
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
        return config.groupId();
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
            ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(config.groupId(), topic);
            String znode = topicDirs.consumerOffsetDir() + "/" + partition.name();
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
            Object cluster = ZkUtils.getCluster(zkClient); // 需要具体实现
            Object broker = ((Cluster)cluster).getBroker(brokerId); // 需要具体实现
            simpleConsumer = new SimpleConsumer(broker.host(), broker.port(),
                    ConsumerConfig.SocketTimeout, ConsumerConfig.SocketBufferSize);
            List<Long> latestOffset = simpleConsumer.getOffsetsBefore(topic, partitionId,
                    OffsetRequest.LatestTime, 1);
            producedOffset = latestOffset.get(0);
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
