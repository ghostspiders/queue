package org.queue.consumer;

import java.util.Map;

/**
 * @author gaoyvfeng
 * @ClassName ConsumerConnector
 * @description:
 * @datetime 2024年 05月 22日 17:39
 * @version: 1.0
 */
public interface ConsumerConnector {
    /**
     * 创建消息流。
     * @param topicCountMap 主题到分区数的映射。
     * @return 按主题分组的Kafka消息流列表。
     */
    Map<String, List<KafkaMessageStream>> createMessageStreams(Map<String, Integer> topicCountMap);

    /**
     * 提交此连接器连接的所有代理分区的偏移量。
     */
    void commitOffsets();

    /**
     * 关闭连接器。
     */
    void shutdown();
}
