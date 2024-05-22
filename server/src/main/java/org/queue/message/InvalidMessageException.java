package org.queue.message;

/**
 * @author gaoyvfeng
 * @ClassName InvalidMessageException
 * @description:
 * @datetime 2024年 05月 22日 10:13
 * @version: 1.0
 */
/**
 * 无效消息异常类，继承自RuntimeException。
 * 表示当消息格式不正确或包含无效数据时抛出的异常。
 */
public class InvalidMessageException extends RuntimeException {

    /**
     * 使用指定的错误信息构造InvalidMessageException异常。
     *
     * @param message 错误信息，描述异常的原因。
     */
    public InvalidMessageException(String message) {
        super(message); // 调用父类RuntimeException的构造函数，并传入错误信息
    }

    /**
     * 无参构造函数，构造一个带有默认错误信息的InvalidMessageException异常。
     */
    public InvalidMessageException() {
        this("Invalid message received"); // 可以传递一个默认的错误信息
    }
}