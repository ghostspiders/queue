package org.queue.common;

/**
 * @author gaoyvfeng
 * @ClassName InvalidPartitionException
 * @description:
 * @datetime 2024年 05月 22日 09:46
 * @version: 1.0
 */
/**
 * 分区无效异常类，继承自RuntimeException。
 * 表示当消息分区无效或发生错误时抛出的异常。
 */
public class InvalidPartitionException extends RuntimeException {

    /**
     * 使用指定的错误信息构造InvalidPartitionException异常。
     *
     * @param message 错误信息，描述异常的原因。
     */
    public InvalidPartitionException(String message) {
        super(message); // 调用父类RuntimeException的构造函数，并传入错误信息
    }

    /**
     * 无参构造函数，构造一个带有null错误信息的InvalidPartitionException异常。
     */
    public InvalidPartitionException() {
        this(null); // 调用带消息的构造函数，并传入null作为默认错误信息
    }
}