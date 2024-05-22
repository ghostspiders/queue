package org.queue.common;

/**
 * @author gaoyvfeng
 * @ClassName UnknownException
 * @description:
 * @datetime 2024年 05月 22日 10:03
 * @version: 1.0
 */
/**
 * 未知异常类，继承自RuntimeException。
 * 表示当发生未知错误或无法确定具体异常类型时抛出的异常。
 */
public class UnknownException extends RuntimeException {

    /**
     * 使用指定的错误信息构造UnknownException异常。
     *
     * @param message 错误信息，描述异常的原因。
     */
    public UnknownException(String message) {
        super(message); // 调用父类RuntimeException的构造函数，并传入错误信息
    }

    /**
     * 无参构造函数，构造一个带有默认错误信息的UnknownException异常。
     */
    public UnknownException() {
        this("Unknown exception occurred"); // 可以传递一个默认的错误信息
    }
}
