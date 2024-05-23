package org.queue.common;

/**
 * @author gaoyvfeng
 * @ClassName UnavailableProducerException
 * @description:
 * @datetime 2024年 05月 22日 09:49
 * @version: 1.0
 */
/**
 * 生产者不可用异常类，继承自RuntimeException。
 * 表示当生产者（发送消息的客户端或服务）不可用时抛出的异常。
 */
public class UnavailableProducerException extends RuntimeException {

    /**
     * 使用指定的错误信息构造UnavailableProducerException异常。
     *
     * @param message 错误信息，描述异常的原因。
     */
    public UnavailableProducerException(String message) {
        super(message); // 调用父类RuntimeException的构造函数，并传入错误信息
    }

    /**
     * 无参构造函数，构造一个带有null错误信息的UnavailableProducerException异常。
     */
    public UnavailableProducerException() {
        this(null); // 调用带消息的构造函数，并传入null作为默认错误信息
    }
}