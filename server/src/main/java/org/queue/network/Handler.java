package org.queue.network;

import java.util.Optional;

/**
 * @author gaoyvfeng
 * @ClassName Handler
 * @description:
 * @datetime 2024年 05月 23日 17:13
 * @version: 1.0
 */
public class Handler {

    /**
     * 定义一个处理器（Handler），它将传入的请求转换为传出的响应。
     * 使用Java的Function接口来表示这个函数关系。
     */
    public interface HandlerType {
        // 将接收到的请求转换为可选的发送响应
        Optional<Send> handleRequest(Receive receive);
    }

    /**
     * 定义一个处理器映射（HandlerMapping），它根据给定的请求找到正确的处理器函数。
     * 使用Java的BiFunction接口来表示这个映射关系。
     */
    public interface HandlerMapping {
        // 根据short类型的标识符和接收到的请求找到对应的处理器
        Handler getHandler(int identifier, Receive receive);
    }
}
