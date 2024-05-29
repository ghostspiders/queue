package org.queue.consumer;

/**
 * @author gaoyvfeng
 * @ClassName ConsumerIterator
 * @description:
 * @datetime 2024年 05月 22日 17:58
 * @version: 1.0
 */
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.queue.message.Message;
import org.queue.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一个迭代器，当从提供的队列中读取到值时，该迭代器会阻塞。
 * 迭代器接收一个shutdownCommand对象，该对象可以被添加到队列中以触发关闭。
 */
class ConsumerIterator implements Iterator<Message> {

    private final BlockingQueue<FetchedDataChunk> channel; // 阻塞队列，用于获取数据块
    private final int consumerTimeoutMs; // 消费者超时时间（毫秒）
    private Iterator<MessageAndOffset> current; // 当前消息迭代器
    private FetchedDataChunk currentDataChunk; // 当前数据块
    private PartitionTopicInfo currentTopicInfo; // 当前主题信息
    private long consumedOffset; // 当前已消费的偏移量
    private static final Message allDone = null; // 表示迭代器完成的标志
    private final Logger logger = LoggerFactory.getLogger(ConsumerIterator.class);

    public ConsumerIterator(BlockingQueue<FetchedDataChunk> channel, int consumerTimeoutMs) {
        this.channel = channel;
        this.consumerTimeoutMs = consumerTimeoutMs;
        this.current = null;
        this.currentDataChunk = null;
        this.currentTopicInfo = null;
        this.consumedOffset = -1L;
    }

    @Override
    public boolean hasNext() {
        // 如果当前迭代器为null或没有更多元素，则尝试获取新的数据块
        while (current == null || !current.hasNext()) {
            currentDataChunk = (consumerTimeoutMs < 0)
                    ? channel.take() // 没有超时限制，阻塞等待数据块
                    : channel.poll(consumerTimeoutMs, TimeUnit.MILLISECONDS); // 有超时限制，尝试获取数据块

            if (currentDataChunk == null) {
                throw new ConsumerTimeoutException("Consumer timeout waiting for data"); // 超时异常
            }

            if (currentDataChunk.isShutdownCommand()) { // 如果接收到关闭命令
                if (logger.isDebugEnabled()) {
                    logger.debug("Received the shutdown command");
                }
                channel.offer(currentDataChunk); // 重新放回关闭命令
                return false; // 迭代器完成
            } else {
                currentTopicInfo = currentDataChunk.getTopicInfo(); // 获取当前主题信息
                if (currentTopicInfo.getConsumeOffset() != currentDataChunk.getFetchOffset()) {
                    // 如果已消费的偏移量和获取的偏移量不一致
                    logger.error("Consumed offset: " + currentTopicInfo.getConsumeOffset()
                            + " doesn't match fetch offset: " + currentDataChunk.getFetchOffset()
                            + " for " + currentTopicInfo + "; Consumer may lose data");
                    currentTopicInfo.resetConsumeOffset(currentDataChunk.getFetchOffset()); // 重置消费偏移量
                }
                current = currentDataChunk.getMessages().iterator(); // 获取消息迭代器
            }
        }
        return true;
    }

    @Override
    public Message next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Message message = makeNext();
        if (consumedOffset < 0) {
            throw new IllegalStateException("Offset returned by the message set is invalid " + consumedOffset);
        }
        currentTopicInfo.setConsumeOffset(consumedOffset); // 设置消费偏移量
        if (logger.isTraceEnabled()) {
            logger.trace("Setting consumed offset to " + consumedOffset);
        }
        return message;
    }

    protected Message makeNext() {
        // 如果当前迭代器不存在或没有更多元素，则需要获取新的迭代器
        while (current == null || !current.hasNext()) {
            try {
                currentDataChunk = (consumerTimeoutMs < 0)
                        ? channel.take() // 如果没有设置超时时间，则阻塞等待数据块
                        : channel.poll(consumerTimeoutMs, TimeUnit.MILLISECONDS); // 如果设置了超时时间，则尝试在指定时间内获取数据块

                // 如果在指定的超时时间内没有获取到数据块，则抛出超时异常
                if (currentDataChunk == null) {
                    throw new ConsumerTimeoutException("Consumer timeout while waiting for data");
                }

                // 如果获取的数据块是关闭命令，则记录日志信息，并重新放回关闭命令到队列，返回迭代结束标志
                if (currentDataChunk.isShutdownCommand()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Received the shutdown command");
                    }
                    channel.offer(currentDataChunk);
                    return allDone; // allDone 代表迭代结束
                } else {
                    // 获取当前主题信息
                    currentTopicInfo = currentDataChunk.getTopicInfo();
                    // 如果当前消费的偏移量与获取的偏移量不一致，则记录错误日志，并重置消费偏移量
                    if (currentTopicInfo.getConsumeOffset() != currentDataChunk.getFetchOffset()) {
                        logger.error("Consumed offset: " + currentTopicInfo.getConsumeOffset() +
                                " doesn't match fetch offset: " + currentDataChunk.getFetchOffset() +
                                " for " + currentTopicInfo + "; Consumer may lose data");
                        currentTopicInfo.resetConsumeOffset(currentDataChunk.getFetchOffset());
                    }
                    // 从获取的数据块中创建消息迭代器
                    current = currentDataChunk.messages.iterator();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // 重置中断状态
                throw new RuntimeException("Interrupted while waiting for data", e);
            }
        }
        // 获取下一个消息和偏移量
        MessageAndOffset item = current.next();
        consumedOffset = item.offset;
        return item.message;
    }

}