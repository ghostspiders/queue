package org.queue.utils;

import java.util.Arrays;
import java.util.Properties;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.StringSerializer;
import org.queue.api.OffsetRequest;
import org.queue.consumer.ConsumerConfig;
import org.queue.cluster.Cluster;
import org.queue.javaapi.consumer.SimpleConsumer;
import org.queue.utils.ZkUtils;
import org.queue.common.Partition;
import org.queue.common.OffsetRequest;
import org.queue.zookeeper.ZKGroupTopicDirs;

public class UpdateOffsetsInZK {

    // 更新ZooKeeper中每个Broker分区的偏移量为最新日志段文件的偏移量
    public static final String Earliest = "earliest"; // 代表最早的偏移量
    public static final String Latest = "latest"; // 代表最新的偏移量

    public static void main(String[] args) {
        // 参数个数检查
        if (args.length < 3) {
            usage();
        }
        // 创建消费者配置对象
        ConsumerConfig config = new ConsumerConfig(Utils.loadProps(args[1]));
        // 创建ZooKeeper客户端
        ZkClient zkClient = new ZkClient(
                config.zkConnect(),
                config.zkSessionTimeoutMs(),
                config.zkConnectionTimeoutMs(),
                new StringSerializer()
        );
        // 根据命令行参数更新偏移量
        switch (args[0]) {
            case Earliest:
                getAndSetOffsets(
                        zkClient,
                        OffsetRequest.EarliestTime(), // 获取最早的偏移量
                        config,
                        args[2]
                );
                break;
            case Latest:
                getAndSetOffsets(
                        zkClient,
                        OffsetRequest.LatestTime(), // 获取最新的偏移量
                        config,
                        args[2]
                );
                break;
            default:
                usage();
        }
    }

    // 获取并设置偏移量的私有方法
    private static void getAndSetOffsets(
            ZkClient zkClient,
            long offsetOption,
            ConsumerConfig config,
            String topic) {
        // 获取集群信息
        Cluster cluster = ZkUtils.getCluster(zkClient);
        // 获取指定主题的分区列表
        List<Partition> partitionsPerTopicMap = ZkUtils.getPartitionsForTopics(zkClient, Collections.singleton(topic)).get(topic);
        List<String> partitions = new ArrayList<>();

        // 获取分区信息
        if (partitionsPerTopicMap != null) {
            for (Partition part : partitionsPerTopicMap) {
                partitions.add(part.toString());
            }
            Collections.sort(partitions); // 对分区进行排序
        } else {
            throw new RuntimeException("无法找到主题 " + topic);
        }

        int numParts = 0;
        // 遍历分区列表
        for (String partString : partitions) {
            Partition part = Partition.parse(partString);
            // 创建简单消费者客户端
            SimpleConsumer consumer = new SimpleConsumer(
                    cluster.getBroker(part.brokerId).host(),
                    cluster.getBroker(part.brokerId).port(),
                    10000,
                    100 * 1024
            );
            // 获取偏移量
            long[] offsets = consumer.getOffsetsBefore(topic, part.partId(), offsetOption, 1);
            // 获取ZooKeeper分组主题目录
            ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(config.groupId(), topic);

            // 打印更新偏移量信息
            System.out.println("更新分区 " + part.name() + " 的新偏移量: " + offsets[0]);
            // 在ZooKeeper中更新偏移量路径
            ZkUtils.updatePersistentPath(
                    zkClient,
                    topicDirs.consumerOffsetDir() + "/" + part.name(),
                    String.valueOf(offsets[0])
            );
            numParts++;
        }
        // 打印更新的分区数量
        System.out.println("更新了 " + numParts + " 个分区的偏移量");
    }

    // 打印用法信息的私有方法
    private static void usage() {
        System.out.println("用法: " + UpdateOffsetsInZK.class.getName() + " [earliest | latest] consumer.properties 主题");
        System.exit(1);
    }
}