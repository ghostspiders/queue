package org.queue.producer;

/**
 * @author gaoyvfeng
 * @ClassName Producer
 * @description:
 * @datetime 2024年 05月 24日 16:40
 * @version: 1.0
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

// 假设存在以下类和接口的实现：
// - ProducerConfig
// - Partitioner
// - ProducerPool
// - BrokerPartitionInfo
// - Broker
// - Partition
// - ProducerData
// - InvalidConfigException
// - InvalidPartitionException
// - NoBrokersForPartitionException
// - ZKBrokerPartitionInfo
// - ZKConfig
// - Utils

public class Producer<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    private final ProducerConfig config;
    private final Partitioner<K> partitioner;
    private final ProducerPool<V> producerPool;
    private final boolean populateProducerPool;
    private final BrokerPartitionInfo brokerPartitionInfo;
    private final AtomicBoolean hasShutdown = new AtomicBoolean(false);
    private final Random random = new Random();

    public Producer(ProducerConfig config, Partitioner<K> partitioner, ProducerPool<V> producerPool,
                    boolean populateProducerPool, BrokerPartitionInfo brokerPartitionInfo) {
        this.config = config;
        this.partitioner = partitioner;
        this.producerPool = producerPool;
        this.populateProducerPool = populateProducerPool;
        this.brokerPartitionInfo = brokerPartitionInfo;
        checkConfig();
        if (brokerPartitionInfo == null) {
            initBrokerPartitionInfo();
        }
        if (populateProducerPool) {
            populateProducerPool();
        }
    }

    // 检查配置是否包含必要的属性
    private void checkConfig() {
        if (!Utils.propertyExists(config.zkConnect) && !Utils.propertyExists(config.brokerPartitionInfo)) {
            throw new InvalidConfigException("At least one of zk.connect or broker.list must be specified");
        }
    }

    // 初始化代理分区信息
    private void initBrokerPartitionInfo() {
        // 根据是否启用ZK来初始化brokerPartitionInfo
        // 这里只是一个示例逻辑
        if (Utils.propertyExists(config.zkConnect)) {
            Properties zkProps = new Properties();
            zkProps.put("zk.connect", config.zkConnect);
            // ... 其他ZK相关属性
            brokerPartitionInfo = new ZKBrokerPartitionInfo(new ZKConfig(zkProps), this::producerCbk);
        } else {
            brokerPartitionInfo = new ConfigBrokerPartitionInfo(config);
        }
    }

    // 填充生产者池
    private void populateProducerPool() {
        Map<Integer, Broker> allBrokers = brokerPartitionInfo.getAllBrokerInfo();
        for (Map.Entry<Integer, Broker> entry : allBrokers.entrySet()) {
            producerPool.addProducer(new Broker(entry.getValue().getId(), entry.getValue().getHost(), entry.getValue().getPort()));
        }
    }


    /**
     * 发送消息，根据键分区到指定主题。
     * @param producerData 包含topic、键和消息数据的ProducerData对象的可变参数。
     */
    public void send(ProducerData<K, V>... producerData) {
        List<ProducerPoolData> producerPoolRequests = new ArrayList<>();
        for (ProducerData<K, V> pd : producerData) {
            // 查找该主题注册的代理分区数量
            logger.debug("Getting the number of broker partitions registered for topic: " + pd.getTopic());
            SortedSet<Partition> numBrokerPartitions = brokerPartitionInfo.getBrokerPartitionInfo(pd.getTopic());
            int totalNumPartitions = numBrokerPartitions.size();
            logger.debug("Broker partitions registered for topic: " + pd.getTopic() + " = " + totalNumPartitions);
            if (totalNumPartitions == 0) {
                throw new NoBrokersForPartitionException("Partition = " + pd.getKey());
            }

            Partition brokerIdPartition = null;
            int partition = 0;
            if (zkEnabled) {
                // 获取分区ID
                int partitionId = getPartition(pd.getKey(), totalNumPartitions);
                brokerIdPartition = numBrokerPartitions.toArray(new Partition[0])[partitionId];
                Broker brokerInfo = brokerPartitionInfo.getBrokerInfo(brokerIdPartition.getBrokerId()).get();
                logger.debug("Sending message to broker " + brokerInfo.getHost() + ":" + brokerInfo.getPort() +
                        " on partition " + brokerIdPartition.getPartId());
                partition = brokerIdPartition.getPartId();
            } else {
                // 随机选择一个代理
                int randomBrokerId = random.nextInt(totalNumPartitions);
                brokerIdPartition = numBrokerPartitions.toArray(new Partition[0])[randomBrokerId];
                Broker brokerInfo = brokerPartitionInfo.getBrokerInfo(brokerIdPartition.getBrokerId()).get();
                logger.debug("Sending message to broker " + brokerInfo.getHost() + ":" + brokerInfo.getPort() +
                        " on a randomly chosen partition");
                partition = ProducerRequest.RandomPartition; // 假设这是定义在某处的一个常量
            }
            // 获取生产者池数据
            ProducerPoolData data = producerPool.getProducerPoolData(pd.getTopic(),
                    new Partition(brokerIdPartition.getBrokerId(), partition),
                    pd.getData());
            producerPoolRequests.add(data);
        }
        // 发送所有收集的生产者池数据
        producerPool.send(producerPoolRequests.toArray(new ProducerPoolData[0]));
    }

    // 根据分区键获取分区ID
    private int getPartition(K key, int numPartitions) {
        if (numPartitions <= 0) {
            throw new InvalidPartitionException("Invalid number of partitions: " + numPartitions);
        }
        int partition = (key == null) ? random.nextInt(numPartitions) : partitioner.partition(key, numPartitions);
        if (partition < 0 || partition >= numPartitions) {
            throw new InvalidPartitionException("Invalid partition id: " + partition);
        }
        return partition;
    }

    // 添加新的生产者到生产者池的回调
    private void producerCbk(int bid, String host, int port) {
        if (populateProducerPool) {
            producerPool.addProducer(new Broker(bid, host, port));
        } else {
            logger.debug("Skipping the callback since populateProducerPool = false");
        }
    }

    // 关闭生产者资源
    public void close() {
        if (hasShutdown.compareAndSet(false, true)) {
            producerPool.close();
            if (brokerPartitionInfo != null) {
                brokerPartitionInfo.close();
            }
        }
    }

}
