package org.queue.common;

/**
 * @author gaoyvfeng
 * @ClassName NoBrokersForPartitionException
 * @description: TODO
 * @datetime 2024年 05月 22日 09:47
 * @version: 1.0
 */
/**
 * 没有Broker可用于分区异常类，继承自RuntimeException。
 * 表示当系统中没有Broker可用于特定分区时抛出的异常。
 */
public class NoBrokersForPartitionException extends RuntimeException {

    /**
     * 使用指定的错误信息构造NoBrokersForPartitionException异常。
     *
     * @param message 错误信息，描述异常的原因。
     */
    public NoBrokersForPartitionException(String message) {
        super(message); // 调用父类RuntimeException的构造函数，并传入错误信息
    }

    /**
     * 无参构造函数，构造一个带有null错误信息的NoBrokersForPartitionException异常。
     */
    public NoBrokersForPartitionException() {
        this(null); // 调用带消息的构造函数，并传入null作为默认错误信息
    }
}
