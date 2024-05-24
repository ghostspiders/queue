package org.queue.producer.async;

/**
 * @author gaoyvfeng
 * @ClassName MissingConfigException
 * @description:
 * @datetime 2024年 05月 24日 15:30
 * @version: 1.0
 */
/**
 * 缺少配置异常，当配置信息缺失时抛出。
 */
public class MissingConfigException extends RuntimeException {
    /**
     * 使用指定的错误消息构造一个新的MissingConfigException。
     * @param message 错误消息
     */
    public MissingConfigException(String message) {
        super(message);
    }

    /**
     * 构造一个新的MissingConfigException，没有错误消息。
     */
    public MissingConfigException() {
        this(null);
    }
}