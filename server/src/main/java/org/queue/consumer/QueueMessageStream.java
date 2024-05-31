package org.queue.consumer;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import org.queue.message.Message;

public class QueueMessageStream implements Iterable<Message> {
    private  BlockingQueue<FetchedDataChunk> queue; // 阻塞队列，用于存储获取的数据块
    private  int consumerTimeoutMs; // 消费者超时时间（毫秒）
    private final ConsumerIterator iter; // 消息迭代器

    /**
     * 构造函数。
     * @param queue 阻塞队列，用于存储获取的数据块。
     * @param consumerTimeoutMs 消费者超时时间（毫秒）。
     */
    public QueueMessageStream(BlockingQueue<FetchedDataChunk> queue, int consumerTimeoutMs) {
        this.queue = queue;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.iter = new ConsumerIterator(queue, consumerTimeoutMs); // 初始化消息迭代器
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