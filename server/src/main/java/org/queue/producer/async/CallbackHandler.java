package org.queue.producer.async;

/**
 * @author gaoyvfeng
 * @ClassName CallbackHandler
 * @description:
 * @datetime 2024年 05月 24日 15:14
 * @version: 1.0
 */


import java.util.Properties;
import java.util.List;

/**
 * 异步生产者中使用的回调处理器API。目的是在异步生产者的数据流经管道的各个阶段时，
 * 提供一些回调处理，以便用户可以插入自定义功能。
 */
public interface CallbackHandler<T> {

    /**
     * 使用Properties对象初始化回调处理器。
     * @param props 用于初始化回调处理器的属性
     */
    void init(Properties props);

    /**
     * 数据进入异步生产者的批处理队列之前调用的回调。
     * @param data 发送到生产者的数据
     * @return 进入队列的处理后的数据
     */
    QueueItem<T> beforeEnqueue(QueueItem<T> data);

    /**
     * 数据进入异步生产者的批处理队列后调用的回调。
     * @param data 发送到生产者的数据
     * @param added 标志，指示数据是否成功添加到队列
     */
    void afterEnqueue(QueueItem<T> data, boolean added);

    /**
     * 异步生产者的后台发送线程出队数据项后调用的回调。
     * @param data 从异步生产者队列中出队的数据项
     * @return 要添加到事件处理器处理的数据项的处理后的列表
     */
    List<QueueItem<T>> afterDequeuingExistingData(QueueItem<T> data);

    /**
     * 事件处理器的handle() API发送批处理数据之前调用的回调。
     * @param data 由事件处理器接收的批处理数据
     * @return 由事件处理器的handle() API发送的处理后的批处理数据
     */
    List<QueueItem<T>> beforeSendingData(List<QueueItem<T>> data);

    /**
     * 生产者发送线程关闭前处理最后一批次数据的回调。
     * @return 发送到事件处理器的最后一批次数据
     */
    List<QueueItem<T>> lastBatchBeforeClose();

    /**
     * 清理并关闭回调处理器
     */
    void close();
}
