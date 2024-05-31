//package org.queue.javaapi.consumer;
//
//import org.queue.consumer.ConsumerConfig;
//import org.queue.consumer.QueueMessageStream;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// * 消费者连接器，使用Zookeeper进行协调。
// */
//public class ZookeeperConsumerConnector implements ConsumerConnector {
//    private final org.queue.consumer.ZookeeperConsumerConnector underlying;
//    private final ConsumerConfig config;
//    private final boolean enableFetcher;
//
//    /**
//     * 构造函数，创建一个新的ZookeeperConsumerConnector。
//     *
//     * @param config        消费者配置。
//     * @param enableFetcher 是否启用数据获取器。
//     */
//    public ZookeeperConsumerConnector(ConsumerConfig config, boolean enableFetcher) {
//        super(); // 调用父类构造函数，如果有的话
//        this.config = config;
//        this.enableFetcher = enableFetcher;
//        this.underlying = new org.queue.consumer.ZookeeperConsumerConnector(config, enableFetcher);
//    }
//
//    /**
//     * 为了方便Java客户端使用，提供带单个参数的构造函数，启用数据获取器。
//     *
//     * @param config 消费者配置。
//     */
//    public ZookeeperConsumerConnector(ConsumerConfig config) {
//        this(config, true);
//    }
//
//    /**
//     * 为Java客户端创建消息流。
//     *
//     * @param topicCountMap 主题到消费数量的映射表。
//     * @return Java风格的消息流映射表。
//     */
//    public Map<String, List<QueueMessageStream>> createMessageStreams(Map<String, Integer> topicCountMap) {
//        Map<String, Integer> scalaTopicCountMap = new HashMap<>();
//        topicCountMap.forEach((k, v) -> scalaTopicCountMap.put(k, v));
//
//        Map<String, List<QueueMessageStream>> scalaReturn = underlying.consume(scalaTopicCountMap);
//        Map<String, List<QueueMessageStream>> ret = new HashMap<>();
//
//        for (Map.Entry<String, List<QueueMessageStream>> entry : scalaReturn.entrySet()) {
//            List<QueueMessageStream> javaStreamList = new ArrayList<>(entry.getValue());
//            ret.put(entry.getKey(), javaStreamList);
//        }
//
//        return ret;
//    }
//    /**
//     * 提交偏移量。
//     */
//    public void commitOffsets() {
//        underlying.commitOffsets();
//    }
//
//    /**
//     * 关闭连接器，释放资源。
//     */
//    public void shutdown() {
//        underlying.shutdown();
//    }
//}