package org.queue.producer;

/**
 * @author gaoyvfeng
 * @ClassName BrokerTopicsListener
 * @description:
 * @datetime 2024年 05月 24日 17:56
 * @version: 1.0
 */
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.queue.cluster.Broker;
import org.queue.cluster.Partition;
import org.queue.utils.ZkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BrokerTopicsListener implements IZkChildListener {
    private final Map<String, SortedSet<Partition>> originalBrokerTopicsPartitionsMap;
    private Map<String, SortedSet<Partition>> oldBrokerTopicPartitionsMap;
    private final Map<Integer, Broker> originalBrokerIdMap;
    private Map<Integer, Broker> oldBrokerIdMap;
    private static final Logger logger = LoggerFactory.getLogger(BrokerTopicsListener.class);
    private ReentrantLock zkWatcherLock;
    private ZkClient zkClient;
    private Map<String, SortedSet<Partition>> topicBrokerPartitions;
    private Map<Integer, Broker> allBrokers;
    private boolean populateProducerPool;
    private ProducerPool producerPool;
    public BrokerTopicsListener(Map<String, SortedSet<Partition>> originalBrokerTopicsPartitionsMap,
                                Map<Integer, Broker> originalBrokerIdMap, ReentrantLock zkWatcherLock, ZkClient zkClient, Map<String, SortedSet<Partition>> topicBrokerPartitions, Map<Integer, Broker> allBrokers,
            boolean populateProducerPool, ProducerPool producerPool) {
        this.originalBrokerTopicsPartitionsMap = new ConcurrentHashMap<>(originalBrokerTopicsPartitionsMap);
        this.oldBrokerTopicPartitionsMap = new ConcurrentHashMap<>(this.originalBrokerTopicsPartitionsMap);
        this.originalBrokerIdMap = new ConcurrentHashMap<>(originalBrokerIdMap);
        this.oldBrokerIdMap = new ConcurrentHashMap<>(this.originalBrokerIdMap);
        this.zkWatcherLock = zkWatcherLock;
        this.zkClient = zkClient;
        this.topicBrokerPartitions = topicBrokerPartitions;
        this.allBrokers = allBrokers;
        this.populateProducerPool = populateProducerPool;
        this.producerPool = producerPool;
        logger.debug("[BrokerTopicsListener] Creating broker topics listener to watch the following paths - \n" +
                "/broker/topics, /broker/topics/topic, /broker/ids");
        logger.debug("[BrokerTopicsListener] Initialized this broker topics listener with initial mapping of broker id to " +
                "partition id per topic with " + oldBrokerTopicPartitionsMap.toString());
    }
    /**
     * 处理ZooKeeper子节点变化事件。
     * @param parentPath 父路径
     * @param currentChilds 当前子节点列表
     */
    @Override
    public void handleChildChange(String parentPath, List<String> currentChilds) {
        zkWatcherLock.lock();
        try {
            logger.info("Watcher fired for path: " + parentPath);

            switch (parentPath) {
                case "/brokers/topics": // 这是/broker/topics路径的监听器
                    Set<String> updatedTopics = currentChilds.stream().collect(Collectors.toSet());
                    logger.info(String.format("[BrokerTopicsListener] List of topics changed at %s Updated topics -> %s",
                            parentPath, updatedTopics.toString()));
                    logger.info(String.format("[BrokerTopicsListener] Old list of topics: %s",
                            oldBrokerTopicPartitionsMap.keySet().toString()));
                    logger.info(String.format("[BrokerTopicsListener] Updated list of topics: %s",
                            updatedTopics.toString()));
                    Set<String> newTopics = updatedTopics.stream().filter(
                            oldBrokerTopicPartitionsMap.keySet()::contains).collect(Collectors.toSet());
                    logger.info(String.format("[BrokerTopicsListener] List of newly registered topics: %s",
                            newTopics.toString()));
                    for (String topic : newTopics) {
                        String brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic;
                        List<String> brokerList = ZkUtils.getChildrenParentMayNotExist(zkClient, brokerTopicPath);
                        processNewBrokerInExistingTopic(topic, brokerList);
                        zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath + "/" + topic, this);
                    }
                    break;
                case "/brokers/ids": // 这是/broker/ids路径的监听器
                    logger.info(String.format("[BrokerTopicsListener] List of brokers changed in the queue cluster %s " +
                            "\t Currently registered list of brokers -> %s", parentPath, currentChilds.toString()));
                    processBrokerChange(parentPath, currentChilds.stream().map(String::valueOf).collect(Collectors.toList()));
                    break;
                default:
                    String[] pathSplits = parentPath.split("/");
                    String topic = pathSplits[pathSplits.length - 1];
                    if (pathSplits.length == 4 && pathSplits[2].equals("topics")) {
                        logger.info(String.format("[BrokerTopicsListener] List of brokers changed at %s " +
                                        "\t Currently registered list of brokers -> %s for topic -> %s",
                                parentPath, currentChilds.toString(), topic));
                        processNewBrokerInExistingTopic(topic, currentChilds.stream().map(String::valueOf).collect(Collectors.toList()));
                    }
            }

            // 更新跟踪旧状态值的数据结构
            oldBrokerTopicPartitionsMap = new HashMap<>(topicBrokerPartitions);
            oldBrokerIdMap = new HashMap<>(allBrokers);
        } finally {
            zkWatcherLock.unlock();
        }
    }
    /**
     * 处理代理变更。
     * @param parentPath 父路径
     * @param curChilds 当前子节点列表
     */
    public void processBrokerChange(String parentPath, List<String> curChilds) {
        if (ZkUtils.BrokerIdsPath.equals(parentPath)) {
            Set<Integer> updatedBrokerList = curChilds.stream().map(Integer::parseInt).collect(Collectors.toSet());
            Set<Integer> newBrokers = updatedBrokerList.stream().filter(bid -> !oldBrokerIdMap.containsKey(bid)).collect(Collectors.toSet());
            logger.debug("[BrokerTopicsListener] List of newly registered brokers: " + newBrokers.toString());
            for (Integer bid : newBrokers) {
                String brokerInfo = ZkUtils.readData(zkClient, ZkUtils.BrokerIdsPath + "/" + bid);
                String[] brokerHostPort = brokerInfo.split(":");
                allBrokers.put(bid, new Broker(bid, brokerHostPort[0], brokerHostPort[1], Integer.parseInt(brokerHostPort[2])));
                logger.debug("[BrokerTopicsListener] Invoking the callback for broker: " + bid);
                producerCbk(bid, brokerHostPort[0], Integer.parseInt(brokerHostPort[2]));
            }

            // 从内存中的活动代理列表中移除死亡的代理
            Set<Integer> deadBrokers = oldBrokerIdMap.keySet().stream().filter(bid -> !updatedBrokerList.contains(bid)).collect(Collectors.toSet());
            logger.debug("[BrokerTopicsListener] Deleting broker ids for dead brokers: " + deadBrokers.toString());
            for (Integer bid : deadBrokers) {
                allBrokers.remove(bid);
                // 也从特定主题中移除这个死亡的代理
                topicBrokerPartitions.keySet().forEach(topic -> {
                    SortedSet<Partition> oldBrokerPartitionList = topicBrokerPartitions.get(topic);
                    if (oldBrokerPartitionList != null) {
                        SortedSet<Partition> aliveBrokerPartitionList = oldBrokerPartitionList.stream()
                                .filter(bp -> bp.getBrokerId() != bid)
                                .collect(Collectors.toCollection(TreeSet::new));
                        topicBrokerPartitions.put(topic, aliveBrokerPartitionList);
                        logger.debug(String.format("[BrokerTopicsListener] Removing dead broker ids for topic: %s" +
                                "\t Updated list of broker id, partition id = %s", topic, aliveBrokerPartitionList.toString()));
                    }
                });
            }
        }
    }
    /**
     * 回调函数，用于向生产者池中添加新的生产者。
     * 通常由ZKBrokerPartitionInfo在ZooKeeper中注册新的代理时使用。
     *
     * @param bid 代理的ID
     * @param host 代理的主机名
     * @param port 代理的端口
     */
    private void producerCbk(int bid, String host, int port) {
        // 如果populateProducerPool为true，则向生产者池中添加生产者
        if (populateProducerPool) {
            producerPool.addProducer(new Broker(bid, host, host,port));
        } else {
            // 如果populateProducerPool为false，则记录调试信息并跳过回调
            logger.error("Skipping the callback since populateProducerPool = false");
        }
    }

    /**
     * 生成指定代理列表的映射，从代理ID映射到(代理ID, 分区数)。
     * @param zkClient ZooKeeper客户端
     * @param topic 主题
     * @param brokerList 代理列表
     * @return 代理列表的(代理ID, 分区数)序列
     */
    private static SortedSet<Partition> getBrokerPartitions(ZkClient zkClient, String topic, List<Integer> brokerList) {
        String brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic;
        List<Integer> numPartitions = brokerList.stream()
                .mapToInt(bid -> {
                    try {
                        return Integer.valueOf(ZkUtils.readData(zkClient, brokerTopicPath + "/" + bid));
                    } catch (Exception e) {
                        logger.error("Error reading number of partitions for broker " + bid, e);
                        return 0;
                    }
                }).boxed().collect(Collectors.toList());
        Map<Integer, Integer> brokerPartitions = brokerList.stream()
                .collect(Collectors.toMap(Function.identity(), bid -> numPartitions.get(brokerList.indexOf(bid))));

        SortedSet<Partition> sortedBrokerPartitions = new TreeSet<>((o1, o2) -> o1.getBrokerId() - o2.getBrokerId());
        sortedBrokerPartitions.addAll(brokerPartitions.entrySet().stream()
                .flatMap(entry -> IntStream.rangeClosed(1, entry.getValue()).mapToObj(i -> new Partition(entry.getKey(), i)))
                .collect(Collectors.toList()));
        return sortedBrokerPartitions;
    }
    /**
     * 生成注册在某个主题下的新代理列表的(brokerId, numPartitions)映射。
     * @param topic 主题路径
     * @param curChilds 变更的代理列表
     */
    public void processNewBrokerInExistingTopic(String topic, List<String> curChilds) {
        // 查找该主题的旧代理列表
        SortedSet<Partition> brokersParts = oldBrokerTopicPartitionsMap.get(topic);
        if (brokersParts != null) {
            logger.debug("[BrokerTopicsListener] Old list of brokers: " +
                    brokersParts.stream().mapToInt(Partition::getBrokerId).toArray());
        }

        // 获取更新后的代理列表
        List<Integer> updatedBrokerList = curChilds.stream().map(Integer::parseInt).collect(Collectors.toList());
        SortedSet<Partition> updatedBrokerParts = getBrokerPartitions(zkClient, topic, updatedBrokerList);

        logger.debug("[BrokerTopicsListener] Currently registered list of brokers for topic: " + topic + " are " +
                curChilds.toString());

        // 更新现有代理上的分区数量
        SortedSet<Partition> mergedBrokerParts = new TreeSet<>(updatedBrokerParts);
        if (brokersParts != null) {
            logger.debug("[BrokerTopicsListener] Unregistered list of brokers for topic: " + topic + " are " +
                    brokersParts.toString());
            mergedBrokerParts.addAll(brokersParts);
        }

        // 只保留活跃的代理
        Iterator<Partition> it = mergedBrokerParts.iterator();
        while (it.hasNext()) {
            Partition partition = it.next();
            if (!allBrokers.containsKey(partition.getBrokerId())) {
                it.remove();
            }
        }

        // 更新主题代理分区映射
        topicBrokerPartitions.put(topic, mergedBrokerParts);
        logger.debug("[BrokerTopicsListener] List of broker partitions for topic: " + topic + " are " +
                mergedBrokerParts.toString());
    }
    /**
     * 重置状态。
     */
    public void resetState() {
        // 日志记录重置前的状态
        logger.debug("[BrokerTopicsListener] Before reseting broker topic partitions state "
                + oldBrokerTopicPartitionsMap.toString());

        // 重置主题分区映射状态
        oldBrokerTopicPartitionsMap.clear();
        oldBrokerTopicPartitionsMap.putAll(topicBrokerPartitions.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new TreeSet<>(e.getValue()))));

        // 日志记录重置后的主题分区映射状态
        logger.debug("[BrokerTopicsListener] After reseting broker topic partitions state "
                + oldBrokerTopicPartitionsMap.toString());

        // 日志记录重置前的状态
        logger.debug("[BrokerTopicsListener] Before reseting broker id map state "
                + oldBrokerIdMap.toString());

        // 重置代理ID映射状态
        oldBrokerIdMap.clear();
        oldBrokerIdMap.putAll(allBrokers);

        // 日志记录重置后的代理ID映射状态
        logger.debug("[BrokerTopicsListener] After reseting broker id map state "
                + oldBrokerIdMap.toString());
    }
}