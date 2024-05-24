package org.queue.producer;

/**
 * @author gaoyvfeng
 * @ClassName ZKBrokerPartitionInfo
 * @description:
 * @datetime 2024年 05月 24日 17:51
 * @version: 1.0
 */
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.stream.Collectors;

public class ZKBrokerPartitionInfo {
    private static final Logger logger = LoggerFactory.getLogger(ZKBrokerPartitionInfo.class);

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

}
