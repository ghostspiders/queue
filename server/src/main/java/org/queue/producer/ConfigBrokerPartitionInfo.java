package org.queue.producer;

/**
 * @author gaoyvfeng
 * @ClassName ConfigBrokerPartitionInfo
 * @description:
 * @datetime 2024年 05月 24日 16:02
 * @version: 1.0
 */
import org.queue.cluster.Broker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/**
 * 基于配置信息维护Kafka代理分区的类。
 */
public class ConfigBrokerPartitionInfo implements BrokerPartitionInfo {
    // 日志记录器
    private static final Logger logger = LoggerFactory.getLogger(ConfigBrokerPartitionInfo.class);
    // Kafka生产者配置
    private final ProducerConfig config;
    // 存储代理分区信息的有序集合
    private final SortedSet<Partition> brokerPartitions;
    // 存储所有代理的主机和端口信息的映射
    private final Map<Integer, Broker> allBrokers;

    /**
     * 构造函数，初始化代理分区信息。
     * @param config Kafka生产者配置。
     */
    public ConfigBrokerPartitionInfo(ProducerConfig config) {
        this.config = config;
        this.brokerPartitions = getConfigTopicPartitionInfo();
        this.allBrokers = getConfigBrokerInfo();
    }

    /**
     * 根据主题返回代理分区信息。
     * @param topic 主题名称，此实现中topic参数未使用，总是返回所有代理分区信息。
     * @return 代理分区信息的有序集合。
     */
    @Override
    public SortedSet<Partition> getBrokerPartitionInfo(String topic) {
        return brokerPartitions;
    }

    /**
     * 根据代理ID返回代理的主机和端口信息。
     * @param brokerId 代理ID。
     * @return 代理信息的Optional对象，如果没有找到代理，则返回空。
     */
    @Override
    public Optional<Broker> getBrokerInfo(int brokerId) {
        return Optional.ofNullable(allBrokers.get(brokerId));
    }

    /**
     * 根据生产者配置生成所有代理的(brokerId, numPartitions)序列。
     * @return 代理分区信息的有序集合。
     */
    private SortedSet<Partition> getConfigTopicPartitionInfo() {
        String[] brokerInfoList = config.getBrokerPartitionInfo().split(",");
        if (brokerInfoList.length == 0) {
            throw new InvalidConfigException("broker.list is empty");
        }
        // 验证每个代理信息是否有效 => (brokerId: brokerHost: brokerPort)
        Arrays.stream(brokerInfoList)
                .forEach(bInfo -> {
                    String[] parts = bInfo.split(":");
                    if (parts.length < 3) {
                        throw new InvalidConfigException("broker.list has invalid value");
                    }
                });
        Map<Integer, Integer> brokerPartitions = Arrays.stream(brokerInfoList)
                .collect(Collectors.toMap(
                        bInfo -> Integer.parseInt(bInfo.split(":")[0]),
                        bInfo -> 1
                ));
        SortedSet<Partition> brokerParts = new TreeSet<>();
        brokerPartitions.forEach((brokerId, numPartitions) -> {
            for (int i = 0; i < numPartitions; i++) {
                Partition partition = new Partition(brokerId, i);
                brokerParts.add(partition);
            }
        });
        return brokerParts;
    }

    /**
     * 根据生产者配置生成所有代理的主机和端口信息。
     * @return 代理ID到(host, port)的映射。
     */
    private Map<Integer, Broker> getConfigBrokerInfo() {
        Map<Integer, Broker> brokerInfo = new HashMap<>();
        String[] brokerInfoList = config.getBrokerPartitionInfo().split(",");
        for (String bInfo : brokerInfoList) {
            String[] brokerIdHostPort = bInfo.split(":");
            brokerInfo.put(Integer.parseInt(brokerIdHostPort[0]),
                    new Broker(Integer.parseInt(brokerIdHostPort[0]),
                            brokerIdHostPort[1],
                            Integer.parseInt(brokerIdHostPort[2])));
        }
        return brokerInfo;
    }

}
