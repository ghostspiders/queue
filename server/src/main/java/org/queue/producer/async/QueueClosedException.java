package org.queue.producer.async;

/**
 * @author gaoyvfeng
 * @ClassName QueueClosedException
 * @description:
 * @datetime 2024年 05月 24日 15:36
 * @version: 1.0
 */
/**
 * 队列关闭异常，当尝试对已关闭的队列执行操作时抛出。
 */
public class QueueClosedException extends RuntimeException {
    /**
     * 使用指定的错误消息构造一个新的QueueClosedException。
     * @param message 错误消息
     */
    public QueueClosedException(String message) {
        super(message);
    }

    /**
     * 构造一个新的QueueClosedException，没有错误消息。
     */
    public QueueClosedException() {
        this(null);
    }
}