package org.queue.serializer;

import org.queue.message.Message;

/**
 * @author gaoyvfeng
 * @ClassName StringEncoder
 * @description:
 * @datetime 2024年 05月 23日 15:47
 * @version: 1.0
 */
class StringEncoder implements Encoder<String> {
    @Override
    public Message toMessage(String event) {
        return new Message(event.getBytes()); // 创建一个新的Message对象，其负载是字符串的字节数组
    }
}