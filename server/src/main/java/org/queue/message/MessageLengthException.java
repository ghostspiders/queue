package org.queue.message;

/**
 * @author gaoyvfeng
 * @ClassName MessageLengthException
 * @description:
 * @datetime 2024年 05月 23日 15:01
 * @version: 1.0
 */
/**
 * 消息长度异常类，当消息长度不符合预期时抛出。
 */
public class MessageLengthException extends RuntimeException {
    /**
     * 使用指定的错误信息构造一个新的MessageLengthException。
     *
     * @param message 错误的详细消息。
     */
    public MessageLengthException(String message) {
        // 调用RuntimeException的构造函数，传递错误信息
        super(message);
    }
}