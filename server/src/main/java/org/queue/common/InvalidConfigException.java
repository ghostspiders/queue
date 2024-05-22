package org.queue.common;

/**
 * @author gaoyvfeng
 * @ClassName InvalidConfigException
 * @description:
 * @datetime 2024年 05月 22日 09:45
 * @version: 1.0
 */
/**
 * 配置无效异常类，继承自RuntimeException。
 */
public class InvalidConfigException extends RuntimeException {

    /**
     * 使用错误信息构造InvalidConfigException异常。
     *
     * @param message 错误信息
     */
    public InvalidConfigException(String message) {
        super(message); // 调用父类RuntimeException的构造函数
    }

    /**
     * 无参构造函数，使用null作为默认的错误信息。
     */
    public InvalidConfigException() {
        this(null); // 调用有参构造函数，传入null
    }
}
