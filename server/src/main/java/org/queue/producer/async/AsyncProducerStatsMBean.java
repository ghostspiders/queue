package org.queue.producer.async;

/**
 * @author gaoyvfeng
 * @ClassName AsyncProducerStatsMBean
 * @description:
 * @datetime 2024年 05月 24日 14:41
 * @version: 1.0
 */
/**
 * AsyncProducerStatsMBean 接口定义了异步生产者统计信息的管理接口。
 */
public interface AsyncProducerStatsMBean {
    /**
     * 获取异步生产者队列的大小。
     * @return 队列大小的整数值。
     */
    int getAsyncProducerQueueSize();

    /**
     * 获取由于各种原因被丢弃的事件数量。
     * @return 被丢弃事件的整数值。
     */
    int getAsyncProducerDroppedEvents();
}
