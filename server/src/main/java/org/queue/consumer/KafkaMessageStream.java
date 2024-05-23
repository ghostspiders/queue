package org.queue.consumer;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMessageStream implements Iterable<Message> {
    private final BlockingQueue<FetchedDataChunk> queue; // 阻塞队列，用于存储获取的数据块
    private final int consumerTimeoutMs; // 消费者超时时间（毫秒）
    private final Logger logger = LoggerFactory.getLogger(KafkaMessageStream.class); // 日志记录器
    private final ConsumerIterator<Message> iter; // 消息迭代器

    /**
     * 构造函数。
     * @param queue 阻塞队列，用于存储获取的数据块。
     * @param consumerTimeoutMs 消费者超时时间（毫秒）。
     */
    public KafkaMessageStream(BlockingQueue<FetchedDataChunk> queue, int consumerTimeoutMs) {
        this.queue = queue;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.iter = new ConsumerIterator<>(queue, consumerTimeoutMs); // 初始化消息迭代器
    }

    /**
     * 创建对流中消息的迭代器。
     * @return 消息迭代器。
     */
    @Override
    public Iterator<Message> iterator() {
        return iter; // 返回消息迭代器
    }
}