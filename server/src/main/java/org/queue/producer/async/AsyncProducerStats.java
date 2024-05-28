package org.queue.producer.async;

/**
 * @author gaoyvfeng
 * @ClassName AsyncProducerStats
 * @description:
 * @datetime 2024年 05月 24日 15:13
 * @version: 1.0
 */
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 异步生产者统计信息类，用于跟踪队列大小和丢弃的事件。
 * @param <T> 队列项类型
 */
public class AsyncProducerStats<T> implements AsyncProducerStatsMBean {
    // 队列项类型
    private BlockingQueue<QueueItem<T>> queue;
    // 丢弃事件数量的原子变量
    private AtomicInteger droppedEvents;
    // 事件数量的原子变量
    private AtomicInteger numEvents;

    /**
     * 构造函数，初始化队列和统计信息。
     * @param queue 阻塞队列
     */
    public AsyncProducerStats(BlockingQueue<QueueItem<T>> queue) {
        this.queue = queue;
        this.droppedEvents = new AtomicInteger(0);
        this.numEvents = new AtomicInteger(0);
    }

    /**
     * 获取异步生产者队列的大小。
     * @return 队列大小的整数值。
     */
    @Override
    public int getAsyncProducerQueueSize() {
        return queue.size();
    }

    /**
     * 获取由于各种原因被丢弃的事件数量。
     * @return 被丢弃事件的整数值。
     */
    @Override
    public int getAsyncProducerDroppedEvents() {
        return droppedEvents.get();
    }

    /**
     * 记录一个被丢弃的事件。
     */
    public void recordDroppedEvents() {
        droppedEvents.getAndIncrement();
    }

    /**
     * 记录一个事件。
     */
    public void recordEvent() {
        numEvents.getAndIncrement();
    }
}
