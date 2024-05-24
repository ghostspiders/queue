package org.queue.producer.async;

/**
 * @author gaoyvfeng
 * @ClassName DefaultEventHandler
 * @description:
 * @datetime 2024年 05月 24日 15:22
 * @version: 1.0
 */
import org.queue.producer.ProducerConfig;
import java.util.List;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

/**
 * 默认事件处理器实现，用于分发异步生产者队列中的批处理数据。
 */
public class DefaultEventHandler<T> implements EventHandler<T> {

    private final ProducerConfig config;
    private final CallbackHandler<T> cbkHandler;
    private static final Logger logger = Logger.getLogger(DefaultEventHandler.class.getName());

    public DefaultEventHandler(ProducerConfig config, CallbackHandler<T> cbkHandler) {
        this.config = config;
        this.cbkHandler = cbkHandler;
    }

    @Override
    public void init(Properties props) {
        // 初始化方法，目前为空实现
    }

    @Override
    public void handle(List<QueueItem<T>> events, SyncProducer syncProducer, Encoder<T> serializer) {
        List<QueueItem<T>> processedEvents = events;
        if (cbkHandler != null) {
            processedEvents = cbkHandler.beforeSendingData(processedEvents);
        }
        send(collate(processedEvents), serializer, syncProducer);
    }

    /**
     * 发送消息到指定的主题和分区。
     * @param messagesPerTopic 按主题和分区分组的消息集合
     * @param syncProducer 用于发送消息的同步生产者实例
     */
    private void send(Map<String, Map<Integer, ByteBufferMessageSet>> messagesPerTopic, SyncProducer syncProducer) {
        if (messagesPerTopic != null && !messagesPerTopic.isEmpty()) {
            // 将Map转换为ProducerRequest数组
            ProducerRequest[] requests = messagesPerTopic.entrySet().stream()
                    .flatMap(entry -> entry.getValue().entrySet().stream()
                            .map(partitionEntry -> new ProducerRequest(entry.getKey(), partitionEntry.getKey(), partitionEntry.getValue())))
                    .toArray(ProducerRequest[]::new);

            // 使用同步生产者发送请求
            syncProducer.multiSend(requests);

            // 如果日志记录器启用了跟踪级别日志，记录发送的主题信息
            if (logger.isLoggable(java.util.logging.Level.FINER)) {
                logger.finer("queue producer sent messages for topics " + messagesPerTopic.keySet());
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
    private Map<String, ByteBufferMessageSet> serialize(Map<String, Map<Integer, List<T>>> eventsPerTopic, Encoder<T> serializer) {
        Map<String, ByteBufferMessageSet> messages = new HashMap<>();
        for (Map.Entry<String, Map<Integer, List<T>>> topicEntry : eventsPerTopic.entrySet()) {
            for (Map.Entry<Integer, List<T>> partitionEntry : topicEntry.getValue().entrySet()) {
                List<ByteBuffer> messageList = new ArrayList<>();
                for (T event : partitionEntry.getValue()) {
                    messageList.add(serializer.toMessage(event));
                }
                ByteBufferMessageSet messageSet = createMessageSet(messageList);
                messages.put(topicEntry.getKey() + "-" + partitionEntry.getKey(), messageSet);
            }
        }
        return messages;
    }

    /**
     * 创建消息集合。
     * @param messageList 消息列表
     * @return 消息集合对象
     */
    private ByteBufferMessageSet createMessageSet(List<ByteBuffer> messageList) {
        // 根据配置决定是否启用压缩等
        if (config.isCompressionEnabled() && config.getCompressedTopics().containsAll(topics)) {
            // 启用压缩
            return new ByteBufferMessageSet(config.getCompressionCodec(), messageList);
        } else {
            // 不启用压缩
            return new ByteBufferMessageSet(NoCompressionCodec, messageList);
        }
    }

    /**
     * 聚合事件到特定主题和分区。
     * @param events 待处理的事件列表
     * @return 按主题和分区聚合的事件Map
     */
    private Map<String, Map<Integer, List<T>>> collate(List<QueueItem<T>> events) {
        Map<String, Map<Integer, List<T>>> collatedEvents = new HashMap<>();
        for (QueueItem<T> event : events) {
            String topic = event.getTopic();
            int partition = event.getPartition();
            T data = event.getData();
            collatedEvents.computeIfAbsent(topic, k -> new HashMap<>())
                    .computeIfAbsent(partition, k -> new ArrayList<>())
                    .add(data);
        }
        return collatedEvents;
    }

}
