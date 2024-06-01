package org.queue.producer.async;

/**
 * @author gaoyvfeng
 * @ClassName DefaultEventHandler
 * @description:
 * @datetime 2024年 05月 24日 15:22
 * @version: 1.0
 */
import cn.hutool.core.lang.Pair;
import org.queue.api.ProducerRequest;
import org.queue.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.queue.message.ByteBufferMessageSet;
import org.queue.message.NoCompressionCodec;
import org.queue.producer.ProducerConfig;
import org.queue.producer.SyncProducer;
import org.queue.serializer.Encoder;

import java.io.IOException;
import java.util.*;
import java.nio.ByteBuffer;
import java.util.stream.Collectors;

/**
 * 默认事件处理器实现，用于分发异步生产者队列中的批处理数据。
 */
public class DefaultEventHandler<T> implements EventHandler<T> {

    private final ProducerConfig config;
    private final CallbackHandler<T> cbkHandler;
    private static final Logger logger = LoggerFactory.getLogger(DefaultEventHandler.class.getName());

    public DefaultEventHandler(ProducerConfig config, CallbackHandler<T> cbkHandler) {
        this.config = config;
        this.cbkHandler = cbkHandler;
    }

    @Override
    public void init(Properties props) {
        // 初始化方法，目前为空实现
    }

    @Override
    public void handle(List<QueueItem<T>> events, SyncProducer syncProducer, Encoder<T> serializer) throws IOException {
        List<QueueItem<T>> processedEvents = events;
        if (cbkHandler != null) {
            processedEvents = cbkHandler.beforeSendingData(processedEvents);
        }
        send(serialize(collate(processedEvents), serializer), syncProducer);
    }

    /**
     * 发送消息到指定的主题和分区。
     * @param messagesPerTopic 按主题和分区分组的消息集合
     * @param syncProducer 用于发送消息的同步生产者实例
     */
    private void send(Map<Pair<String, Integer>, ByteBufferMessageSet> messagesPerTopic, SyncProducer syncProducer) throws IOException {
        if (messagesPerTopic != null && !messagesPerTopic.isEmpty()) {
            // 将Map转换为ProducerRequest数组

            ProducerRequest[] requests = messagesPerTopic.entrySet().stream().map(entry -> new ProducerRequest(entry.getKey().getKey(), entry.getKey().getValue(), entry.getValue()))
                    .toArray(ProducerRequest[]::new);
            // 使用同步生产者发送请求
            syncProducer.multiSend(requests);

            // 如果日志记录器启用了跟踪级别日志，记录发送的主题信息
            if (logger.isDebugEnabled()) {
                logger.debug("queue producer sent messages for topics " + messagesPerTopic.keySet());
            }
        }
    }
    @Override
    public void close() {
        // 关闭事件处理器时的清理工作，如果有必要的话
    }

    /**
     * 序列化事件数据。
     * @param eventsPerTopic 按主题和分区分组的事件
     * @param serializer 编码器
     * @return 序列化后的消息集合，按主题和分区分组
     */
    private Map<Pair<String, Integer>, ByteBufferMessageSet>  serialize(Map<Pair<String, Integer>, List<T>> eventsPerTopic, Encoder<T> serializer) {
        Map<Pair<String, Integer>, ByteBufferMessageSet> messages = new HashMap<>();
        for (Map.Entry<Pair<String, Integer>, List<T>> topicEntry : eventsPerTopic.entrySet()) {
            List<Message> messageList = new ArrayList<>();
            for (T event : topicEntry.getValue()) {
                messageList.add(serializer.toMessage(event));
            }
            ByteBufferMessageSet messageSet = createMessageSet(messageList);
            messages.put(topicEntry.getKey(), messageSet);
        }
        return messages;
    }

    /**
     * 创建消息集合。
     * @param messageList 消息列表
     * @return 消息集合对象
     */
    private ByteBufferMessageSet createMessageSet(List<Message> messageList) {
        // 根据配置决定是否启用压缩等
        if (config.getCompressionCodec() != null) {
            // 启用压缩
            return new ByteBufferMessageSet(config.getCompressionCodec(), messageList);
        } else {
            // 不启用压缩
            return new ByteBufferMessageSet(new NoCompressionCodec(), messageList);
        }
    }

    /**
     * 聚合事件到特定主题和分区。
     * @param events 待处理的事件列表
     * @return 按主题和分区聚合的事件Map
     */
    public  Map<Pair<String, Integer>, List<T>> collate(List<QueueItem<T>> events) {
        Map<Pair<String, Integer>, List<T>> collatedEvents = new HashMap<>();
        Set<String> distinctTopics = new HashSet<>();
        Set<Integer> distinctPartitions = new HashSet<>();

        // 提取不同的主题和分区
        for (QueueItem<T> event : events) {
            distinctTopics.add(event.getTopic());
            distinctPartitions.add(event.getPartition());
        }
        for (String topic : distinctTopics) {
            for (Integer partition : distinctPartitions) {
                List<T> topicPartitionEvents = events.stream()
                        .filter(e -> e.getTopic().equals(topic) && e.getPartition() == partition)
                        .map(QueueItem::getData)
                        .collect(Collectors.toList());
                collatedEvents.put(new Pair<>(topic, partition), topicPartitionEvents);
            }
        }
        return collatedEvents;
    }
}
