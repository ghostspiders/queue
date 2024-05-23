package org.queue.common;

/**
 * @author gaoyvfeng
 * @ClassName InvalidMessageSizeException
 * @description:
 * @datetime 2024年 05月 22日 09:46
 * @version: 1.0
 */
/**
 * 消息大小无效异常类，继承自RuntimeException。
 */
public class InvalidMessageSizeException extends RuntimeException {

    /**
     * 使用指定的错误信息构造InvalidMessageSizeException异常。
     *
     * @param message 错误信息，描述异常的原因。
     */
    public InvalidMessageSizeException(String message) {
        super(message); // 调用父类RuntimeException的构造函数，并传入错误信息
    }

    /**
     * 无参构造函数，构造一个带有null错误信息的InvalidMessageSizeException异常。
     */
    public InvalidMessageSizeException() {
        this(null); // 调用带消息的构造函数，并传入null作为默认错误信息
    }
}
