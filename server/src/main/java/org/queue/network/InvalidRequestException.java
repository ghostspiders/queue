package org.queue.network;

// Java中的异常类，继承自RuntimeException
public class InvalidRequestException extends RuntimeException {

    // 主构造函数，接受一个字符串消息作为参数，并将其传递给RuntimeException的构造函数
    public InvalidRequestException(String message) {
        super(message);
    }

    // 辅助构造函数，允许不传递消息时创建异常，此时消息为空字符串
    public InvalidRequestException() {
        this("");
    }
}