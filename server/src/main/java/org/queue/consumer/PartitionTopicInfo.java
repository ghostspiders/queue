package org.queue.consumer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.queue.cluster.Partition;
import org.queue.common.ErrorMapping;
import org.queue.message.ByteBufferMessageSet;

public class PartitionTopicInfo {
    private final String topic; // 主题名称
    private final int brokerId; // 代理ID
    private final Partition partition; // 分区信息
    private final BlockingQueue<FetchedDataChunk> chunkQueue; // 已获取数据块的阻塞队列
    private final AtomicLong consumedOffset; // 已消费的偏移量
    private final AtomicLong fetchedOffset; // 已获取的偏移量
    private final AtomicInteger fetchSize; // 获取的大小

    private final Logger logger = LoggerFactory.getLogger(PartitionTopicInfo.class.getName()); // 日志记录器

    // 构造函数
    public PartitionTopicInfo(String topic, int brokerId, Partition partition,
                              BlockingQueue<FetchedDataChunk> chunkQueue,
                              AtomicLong consumedOffset, AtomicLong fetchedOffset,
                              AtomicInteger fetchSize) {
        this.topic = topic;
        this.brokerId = brokerId;
        this.partition = partition;
        this.chunkQueue = chunkQueue;
        this.consumedOffset = consumedOffset;
        this.fetchedOffset = fetchedOffset;
        this.fetchSize = fetchSize;
        if (logger.isDebugEnabled()) {
            logger.debug("initial consumer offset of " + this + " is " + consumedOffset.get());
            logger.debug("initial fetch offset of " + this + " is " + fetchedOffset.get());
        }
    }

    // 获取已消费的偏移量
    public long getConsumeOffset() {
        return consumedOffset.get();
    }

    // 获取已获取的偏移量
    public long getFetchOffset() {
        return fetchedOffset.get();
    }

    // 重置已消费的偏移量
    public void resetConsumeOffset(long newConsumeOffset) {
        consumedOffset.set(newConsumeOffset);
        if (logger.isDebugEnabled()) {
            logger.debug("reset consume offset of " + this + " to " + newConsumeOffset);
        }
    }

    // 重置已获取的偏移量
    public void resetFetchOffset(long newFetchOffset) {
        fetchedOffset.set(newFetchOffset);
        if (logger.isDebugEnabled()) {
            logger.debug("reset fetch offset of " + this + " to " + newFetchOffset);
        }
    }

    /**
     * 将消息集合排队以供处理
     * @param messages 消息集合
     * @param fetchOffset 获取偏移量
     * @return 有效字节数
     */
    public long enqueue(ByteBufferMessageSet messages, long fetchOffset) throws Throwable {
        long size = messages.shallowValidBytes();
        if (size > 0) {
            // 更新获取的偏移量，以压缩数据块大小为准，而非解压缩的消息集合大小
            if (logger.isDebugEnabled()) {
                logger.debug("Updating fetch offset = " + fetchedOffset.get() + " with size = " + size);
            }
            long newOffset = fetchedOffset.addAndGet(size);
            if (logger.isDebugEnabled()) {
                logger.debug("updated fetch offset of " + this + " to " + newOffset);
            }
            try {
                chunkQueue.put(new FetchedDataChunk(messages, this, fetchOffset));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // 捕获中断异常并恢复中断状态
            }
        }
        return size;
    }

    /**
     * 向队列中添加一个带有异常信息的空消息，以便客户端能够看到错误
     * @param e 异常信息
     * @param fetchOffset 获取偏移量
     */
    public void enqueueError(Throwable e, long fetchOffset) {
        ByteBufferMessageSet messages = new ByteBufferMessageSet(
                ErrorMapping.EmptyByteBuffer,0L,
                ErrorMapping.codeFor(e.getClass())
        );
        try {
            chunkQueue.put(new FetchedDataChunk(messages, this, fetchOffset));
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt(); // 捕获中断异常并恢复中断状态
        }
    }

    // 返回分区主题信息的字符串表示
    @Override
    public String toString() {
        return topic + ":" + partition + ": fetched offset = " + fetchedOffset.get() +
                ": consumed offset = " + consumedOffset.get();
    }

    public String getTopic() {
        return topic;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public Partition getPartition() {
        return partition;
    }

    public BlockingQueue<FetchedDataChunk> getChunkQueue() {
        return chunkQueue;
    }

    public AtomicLong getConsumedOffset() {
        return consumedOffset;
    }

    public AtomicLong getFetchedOffset() {
        return fetchedOffset;
    }

    public AtomicInteger getFetchSize() {
        return fetchSize;
    }
}