package org.queue.common;

/**
 * @author gaoyvfeng
 * @ClassName InvalidTopicException
 * @description:
 * @datetime 2024年 05月 22日 10:09
 * @version: 1.0
 */
/**
 * 主题无效异常类，继承自RuntimeException。
 * 表示当指定的主题无效或无法识别时抛出的异常。
 */
public class InvalidTopicException extends RuntimeException {

    /**
     * 使用指定的错误信息构造InvalidTopicException异常。
     *
     * @param message 错误信息，描述异常的原因。
     */
    public InvalidTopicException(String message) {
        super(message); // 调用父类RuntimeException的构造函数，并传入错误信息
    }

    /**
     * 无参构造函数，构造一个带有null错误信息的InvalidTopicException异常。
     */
    public InvalidTopicException() {
        this(null); // 调用带消息的构造函数，并传入null作为默认错误信息
    }
}