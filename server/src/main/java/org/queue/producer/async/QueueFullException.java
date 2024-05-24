package org.queue.producer.async;

/**
 * @author gaoyvfeng
 * @ClassName QueueFullException
 * @description:
 * @datetime 2024年 05月 24日 15:38
 * @version: 1.0
 */
/**
 * 队列已满异常，当尝试向已满的队列中添加元素时抛出。
 */
public class QueueFullException extends RuntimeException {
    /**
     * 使用指定的错误消息构造一个新的QueueFullException。
     * @param message 错误消息
     */
    public QueueFullException(String message) {
        super(message);
    }

    /**
     * 构造一个新的QueueFullException，没有错误消息。
     */
    public QueueFullException() {
        this(null);
    }
}