package org.queue.producer.async;

/**
 * @author gaoyvfeng
 * @ClassName EventHandler
 * @description:
 * @datetime 2024年 05月 24日 15:18
 * @version: 1.0
 */

import java.util.Properties;
import java.util.List;
import org.queue.producer.SyncProducer;
import org.queue.serializer.Encoder;
/**
 * 用于从异步生产者的队列分发批处理数据的处理程序。
 */
public interface EventHandler<T> {

    /**
     * 使用Properties对象初始化事件处理器。
     * @param props 用于初始化事件处理器的属性
     */
    default void init(Properties props) {
        // 默认实现为空，子类可以覆盖此方法以提供自定义初始化逻辑
    }

    /**
     * 回调方法，用于分发批处理数据并将其发送到队列服务器。
     * @param events 发送到生产者的数据列表
     * @param producer 用于发送数据的底层生产者
     * @param encoder 数据编码器，用于将数据转换为可发送的格式
     */
    void handle(List<QueueItem<T>> events, SyncProducer producer, Encoder<T> encoder);

    /**
     * 清理并关闭事件处理器。
     */
    default void close() {
        // 默认实现为空，子类可以覆盖此方法以提供自定义清理逻辑
    }
}