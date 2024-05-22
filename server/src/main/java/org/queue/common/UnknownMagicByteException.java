package org.queue.common;

/**
 * @author gaoyvfeng
 * @ClassName UnknownMagicByteException
 * @description:
 * @datetime 2024年 05月 22日 10:04
 * @version: 1.0
 */
/**
 * 未知魔术字节异常类，继承自RuntimeException。
 * 表示当遇到无法识别的魔术字节时抛出的异常，通常用于协议版本控制。
 */
public class UnknownMagicByteException extends RuntimeException {

    /**
     * 使用指定的错误信息构造UnknownMagicByteException异常。
     *
     * @param message 错误信息，描述异常的原因。
     */
    public UnknownMagicByteException(String message) {
        super(message); // 调用父类RuntimeException的构造函数，并传入错误信息
    }

    /**
     * 无参构造函数，构造一个带有null错误信息的UnknownMagicByteException异常。
     */
    public UnknownMagicByteException() {
        this(null); // 调用带消息的构造函数，并传入null作为默认错误信息
    }
}