package org.queue.common;

/**
 * @author gaoyvfeng
 * @ClassName UnknownCodecException
 * @description:
 * @datetime 2024年 05月 22日 10:02
 * @version: 1.0
 */
/**
 * 未知编解码器异常类，继承自RuntimeException。
 * 表示当请求的编解码器未知或无法识别时抛出的异常。
 */
public class UnknownCodecException extends RuntimeException {

    /**
     * 使用指定的错误信息构造UnknownCodecException异常。
     *
     * @param message 错误信息，描述异常的原因。
     */
    public UnknownCodecException(String message) {
        super(message); // 调用父类RuntimeException的构造函数，并传入错误信息
    }

    /**
     * 无参构造函数，构造一个带有null错误信息的UnknownCodecException异常。
     */
    public UnknownCodecException() {
        this(null); // 调用带消息的构造函数，并传入null作为默认错误信息
    }
}
