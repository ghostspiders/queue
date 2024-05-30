package org.queue.consumer;

/**
 * @author gaoyvfeng
 * @ClassName ConsumerIterator
 * @description:
 * @datetime 2024年 05月 22日 17:58
 * @version: 1.0
 */
import org.queue.message.Message;
import org.queue.message.MessageAndOffset;
import org.queue.utils.IteratorTemplate;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Logger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

// 假设存在一个Message类，MessageAndOffset类，以及FetchedDataChunk类
// 以及PartitionTopicInfo类和ZookeeperConsumerConnector类

/**
 * 一个迭代器，用于从提供的阻塞队列中读取值。
 * 该迭代器接受一个shutdownCommand对象，该对象可以添加到队列中以触发关闭。
 */
public class ConsumerIterator extends IteratorTemplate<Message> {
    private final BlockingQueue<FetchedDataChunk> channel; // 阻塞队列
    private final int consumerTimeoutMs; // 消费者超时时间（毫秒）
    private Iterator<MessageAndOffset> current  = null; // 当前迭代器
    private FetchedDataChunk currentDataChunk = null; // 当前数据块
    private PartitionTopicInfo currentTopicInfo = null; // 当前主题信息
    private long consumedOffset = -1L; // 已消费的偏移量

    // 日志记录器
    private static final Logger logger = Logger.getLogger(ConsumerIterator.class.getName());

    public ConsumerIterator(BlockingQueue<FetchedDataChunk> channel, int consumerTimeoutMs) {
        this.channel = channel;
        this.consumerTimeoutMs = consumerTimeoutMs;
    }

    @Override
    public Message next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements");
        }
        Message message = super.next();
        if (consumedOffset < 0) {
            throw new IllegalStateException("Offset returned by the message set is invalid " + consumedOffset);
        }
        currentTopicInfo.resetConsumeOffset(consumedOffset);
        if (logger.isLoggable(java.util.logging.Level.FINE)) {
            logger.fine("Setting consumed offset to " + consumedOffset);
        }
        return message;
    }

    protected Message makeNext() throws InterruptedException {
        // 如果当前迭代器为空或没有更多元素，则从阻塞队列中获取新的数据块
        if (current == null || !current.hasNext()) {
            if (consumerTimeoutMs < 0) {
                currentDataChunk = channel.take(); // 阻塞等待数据块
            } else {
                currentDataChunk = channel.poll(consumerTimeoutMs, TimeUnit.MILLISECONDS); // 有超时的等待
                if (currentDataChunk == null) {
                    throw new ConsumerTimeoutException("Consumer timeout after " + consumerTimeoutMs + "ms");
                }
            }

            // 如果接收到关闭命令，则记录日志并将命令放回队列
            if (currentDataChunk == ZookeeperConsumerConnector.shutdownCommand) {
                if (logger.isLoggable(java.util.logging.Level.FINE)) {
                    logger.fine("Received the shutdown command");
                }
                channel.offer(currentDataChunk);
                return allDone();
            } else {
                // 更新当前主题信息
                currentTopicInfo = currentDataChunk.getTopicInfo();
                if (currentTopicInfo.getConsumeOffset() != currentDataChunk.getFetchOffset()) {
                    logger.severe("Consumed offset: " + currentTopicInfo.getConsumeOffset() +
                            " doesn't match fetch offset: " + currentDataChunk.getFetchOffset() +
                            " for " + currentTopicInfo + "; Consumer may lose data");
                    currentTopicInfo.resetConsumeOffset(currentDataChunk.getFetchOffset());
                }
                current = currentDataChunk.getMessages().iterator(); // 创建新的消息迭代器
            }
        }
        // 获取下一个消息
        MessageAndOffset item = current.next();
        consumedOffset = item.getOffset();
        return item.getMessage();
    }
}