package org.queue.javaapi.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Kafka消费者连接器，使用Zookeeper进行协调。
 */
public class ZookeeperConsumerConnector extends ConsumerConnector {
    private final org.queue.consumer.ZookeeperConsumerConnector underlying;
    private final ConsumerConfig config;
    private final boolean enableFetcher;

    /**
     * 构造函数，创建一个新的ZookeeperConsumerConnector。
     *
     * @param config        消费者配置。
     * @param enableFetcher 是否启用数据获取器。
     */
    public ZookeeperConsumerConnector(ConsumerConfig config, boolean enableFetcher) {
        super(); // 调用父类构造函数，如果有的话
        this.config = config;
        this.enableFetcher = enableFetcher;
        this.underlying = new org.queue.consumer.ZookeeperConsumerConnector(config, enableFetcher);
    }

    /**
     * 为了方便Java客户端使用，提供带单个参数的构造函数，启用数据获取器。
     *
     * @param config 消费者配置。
     */
    public ZookeeperConsumerConnector(ConsumerConfig config) {
        this(config, true);
    }

    /**
     * 为Java客户端创建消息流。
     *
     * @param topicCountMap 主题到消费数量的映射表。
     * @return Java风格的消息流映射表。
     */
    public Map<String, List<KafkaMessageStream>> createMessageStreams(Map<String, Integer> topicCountMap) {
        // 将Java的Map转换为Scala的Map，并把Integer转换为Int，这里假设已有相应的方法
        Map<String, Integer> topicCountMap2 = new HashMap<>();
        for (Map.Entry<String, Integer> entry : topicCountMap.entrySet()) {
            topicCountMap2.put(entry.getKey(), entry.getValue());
        }

        // 调用底层的consume方法
        Map<String, List<KafkaStream>> scalaReturn = underlying.consume(topicCountMap2);
        Map<String, List<KafkaMessageStream>> ret = new HashMap<>();

        // 将Scala的返回结果转换为Java的Map
        for (Map.Entry<String, List<KafkaStream>> entry : scalaReturn.entrySet()) {
            List<KafkaMessageStream> javaStreamList = new ArrayList<>();
            for (KafkaStream stream : entry.getValue()) {
                javaStreamList.add(new KafkaMessageStream(stream)); // 假设KafkaMessageStream有相应的构造函数
            }
            ret.put(entry.getKey(), javaStreamList);
        }
        return ret;
    }

    /**
     * 提交偏移量。
     */
    public void commitOffsets() {
        underlying.commitOffsets();
    }

    /**
     * 关闭连接器，释放资源。
     */
    public void shutdown() {
        underlying.shutdown();
    }
}