package org.queue.producer;

/**
 * @author gaoyvfeng
 * @ClassName ZKBrokerPartitionInfo
 * @description:
 * @datetime 2024年 05月 24日 17:51
 * @version: 1.0
 */
import org.I0Itec.zkclient.ZkClient;
import org.queue.cluster.Broker;
import org.queue.cluster.Partition;
import org.queue.utils.ZKConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ZKBrokerPartitionInfo extends BrokerPartitionInfo {
    private static final Logger logger = LoggerFactory.getLogger(ZKBrokerPartitionInfo.class);
    private final ReentrantLock zkWatcherLock = new ReentrantLock();
    private final ZkClient zkClient;
    private volatile Map<String, List<BrokerPartitionInfo>> topicBrokerPartitions;
    private volatile Map<Integer, Broker> allBrokers;

    public ZKBrokerPartitionInfo(ZKConfig config, ProducerCallback producerCbk) {
        super(config, producerCbk);
        // Assuming ZKConfig and BrokerPartitionInfo are defined elsewhere
        this.zkClient = new ZkClient(config.getZkConnect(), config.getZkSessionTimeoutMs(), config.getZkConnectionTimeoutMs(), new StringSerializer());
        this.topicBrokerPartitions = getZKTopicPartitionInfo();
        this.allBrokers = getZKBrokerInfo();
        initializeListeners();
    }

    // Initialization and registration of listeners would be done in a method like this:
    public void initializeListeners() {
        BrokerTopicsListener listener = new BrokerTopicsListener(topicBrokerPartitions, allBrokers);
        zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, listener);
        for (String topic : topicBrokerPartitions.keySet()) {
            zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath + "/" + topic, listener);
            logger.debug("Registering listener on path: " + ZkUtils.BrokerTopicsPath + "/" + topic);
        }
        zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, listener);
        zkClient.subscribeStateChanges(new ZKSessionExpirationListener(listener));
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
                .mapToDouble(bid -> {
                    try {
                        return Double.parseDouble(ZkUtils.readData(zkClient, brokerTopicPath + "/" + bid));
                    } catch (Exception e) {
                        logger.error("Error reading number of partitions for broker " + bid, e);
                        return 0;
                    }
                }).boxed().collect(Collectors.toList());
        Map<Integer, Integer> brokerPartitions = brokerList.stream()
                .collect(Collectors.toMap(Map.Entry::getKey, bid -> numPartitions.get(brokerList.indexOf(bid))));

        SortedSet<Partition> sortedBrokerPartitions = new TreeSet<>((o1, o2) -> o1.getBrokerId() - o2.getBrokerId());
        sortedBrokerPartitions.addAll(brokerPartitions.entrySet().stream()
                .flatMap(entry -> IntStream.rangeClosed(1, entry.getValue()).mapToObj(i -> new Partition(entry.getKey(), i)))
                .collect(Collectors.toList()));
        return sortedBrokerPartitions;
    }

    /**
     * 根据topic获取Broker分区信息
     * @param topic 需要获取分区信息的topic
     * @return 返回一个有序集合，包含(brokerId, numPartitions)。如果没有Broker可用，则返回空集合。
     */
    public SortedSet<Partition> getBrokerPartitionInfo(String topic) {
        // 获取topic对应的Broker分区信息
        SortedSet<Partition> brokerPartitions = topicBrokerPartitions.get(topic);
        if (brokerPartitions == null || brokerPartitions.isEmpty()) {
            // 如果没有Broker注册到这个topic，或者Broker列表为空，则从现有Broker中启动
            brokerPartitions = bootstrapWithExistingBrokers(topic);
            // 更新topic到Broker分区信息的映射
            topicBrokerPartitions.put(topic, brokerPartitions);
        }
        return brokerPartitions;
    }

    /**
     * 根据brokerId获取Broker信息
     * @param brokerId 需要获取信息的Broker的ID
     * @return 返回包含Broker主机和端口信息的Optional对象。如果没有找到Broker，则返回空的Optional。
     */
    public Optional<Broker> getBrokerInfo(int brokerId) {
        return Optional.ofNullable(allBrokers.get(brokerId));
    }

    /**
     * 获取所有Broker的信息映射
     * @return 返回从Broker ID到Broker主机和端口的映射
     */
    public Map<Integer, Broker> getAllBrokerInfo() {
        return new HashMap<>(allBrokers);
    }

    /**
     * 关闭ZooKeeper客户端连接
     */
    public void close() {
        if (zkClient != null) {
            zkClient.close();
        }
    }
    /**
     * 当topic下没有注册Broker时，使用集群中可用的Broker进行引导。
     * @param topic 需要引导的topic
     * @return 返回SortedSet集合，包含Partition对象。
     */
    private SortedSet<Partition> bootstrapWithExistingBrokers(String topic) {
        logger.debug("Currently, no brokers are registered under topic: " + topic);
        logger.debug("Bootstrapping topic: " + topic + " with available brokers in the cluster with default " +
                "number of partitions = 1");
        List<String> allBrokersIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath);
        logger.trace("List of all brokers currently registered in zookeeper = " + allBrokersIds.toString());
        // 假设每个Broker只有一个分区，即从每个Broker中选择分区0作为候选
        SortedSet<Partition> numBrokerPartitions = new TreeSet<>(Comparator.comparing(Partition::getBrokerId));
        for (String brokerIdStr : allBrokersIds) {
            int brokerId = Integer.parseInt(brokerIdStr);
            numBrokerPartitions.add(new Partition(brokerId, 0));
        }
        logger.debug("Adding following broker id, partition id for NEW topic: " + topic + "=" + numBrokerPartitions.toString());
        return numBrokerPartitions;
    }

    /**
     * 为在ZooKeeper中注册的所有topic生成(brokerId, numPartitions)序列。
     * @return 从topic到(brokerId, numPartitions)序列的映射。
     */
    private Map<String, SortedSet<Partition>> getZKTopicPartitionInfo() {
        Map<String, SortedSet<Partition>> brokerPartitionsPerTopic = new HashMap<>();
        ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.BrokerTopicsPath);
        List<String> topics = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerTopicsPath);
        for (String topic : topics) {
            // 查找注册在此topic上的Broker分区数量
            String brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic;
            List<String> brokerList = ZkUtils.getChildrenParentMayNotExist(zkClient, brokerTopicPath);
            Map<String, Integer> numPartitions = brokerList.stream()
                    .collect(Collectors.toMap(
                            bid -> bid,
                            bid -> Integer.parseInt(ZkUtils.readData(zkClient, brokerTopicPath + "/" + bid)))
                    );
            List<Map.Entry<String, Integer>> brokerPartitions = new ArrayList<>(numPartitions.entrySet());
            brokerPartitions.sort((id1, id2) -> id1.getKey().compareTo(id2.getKey()));
            SortedSet<Partition> sortedBrokerPartitions = new TreeSet<>(Comparator.comparing(Partition::getBrokerId));
            for (Map.Entry<String, Integer> bp : brokerPartitions) {
                for (int i = 0; i < bp.getValue(); i++) {
                    sortedBrokerPartitions.add(new Partition(Integer.parseInt(bp.getKey()), i));
                }
            }
            brokerPartitionsPerTopic.put(topic, sortedBrokerPartitions);
            logger.debug("Sorted list of broker ids and partition ids on each for topic: " + topic + " = " + sortedBrokerPartitions.toString());
        }
        return brokerPartitionsPerTopic;
    }

    /**
     * 为在ZooKeeper中注册的所有Broker生成映射。
     * @return 从brokerId到(host, port)的映射。
     */
    private Map<Integer, Broker> getZKBrokerInfo() {
        Map<Integer, Broker> brokers = new HashMap<>();
        List<String> allBrokerIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath);
        for (String bid : allBrokerIds) {
            int brokerId = Integer.parseInt(bid);
            String brokerInfo = ZkUtils.readData(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId);
            brokers.put(brokerId, Broker.createBroker(brokerId, brokerInfo));
        }
        return brokers;
    }
}
