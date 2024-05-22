package org.queue.common;

/**
 * @author gaoyvfeng
 * @ClassName MessageSizeTooLargeException
 * @description:
 * @datetime 2024年 05月 22日 10:10
 * @version: 1.0
 */
/**
 * 消息大小过大异常类，继承自RuntimeException。
 * 表示当消息的大小超出了系统允许的最大限制时抛出的异常。
 */
public class MessageSizeTooLargeException extends RuntimeException {

    /**
     * 使用指定的错误信息构造MessageSizeTooLargeException异常。
     *
     * @param message 错误信息，描述异常的原因。
     */
    public MessageSizeTooLargeException(String message) {
        super(message); // 调用父类RuntimeException的构造函数，并传入错误信息
    }

    /**
     * 无参构造函数，构造一个带有null错误信息的MessageSizeTooLargeException异常。
     */
    public MessageSizeTooLargeException() {
        this(null); // 调用带消息的构造函数，并传入null作为默认错误信息
    }
}