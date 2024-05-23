package org.queue.common;

/**
 * @author gaoyvfeng
 * @ClassName OffsetOutOfRangeException
 * @description:
 * @datetime 2024年 05月 22日 09:48
 * @version: 1.0
 */
/**
 * 偏移量超出范围异常类，继承自RuntimeException。
 * 表示当指定的偏移量超出了允许的范围时抛出的异常。
 */
public class OffsetOutOfRangeException extends RuntimeException {

    /**
     * 使用指定的错误信息构造OffsetOutOfRangeException异常。
     *
     * @param message 错误信息，描述异常的原因。
     */
    public OffsetOutOfRangeException(String message) {
        super(message); // 调用父类RuntimeException的构造函数，并传入错误信息
    }

    /**
     * 无参构造函数，构造一个带有null错误信息的OffsetOutOfRangeException异常。
     */
    public OffsetOutOfRangeException() {
        this(null); // 调用带消息的构造函数，并传入null作为默认错误信息
    }
}
