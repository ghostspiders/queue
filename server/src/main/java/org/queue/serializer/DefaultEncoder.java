package org.queue.serializer;

import org.queue.message.Message;

/**
 * @author gaoyvfeng
 * @ClassName DefaultEncoder
 * @description:
 * @datetime 2024年 05月 23日 15:46
 * @version: 1.0
 */
class DefaultEncoder implements Encoder<Message> {
    @Override
    public Message toMessage(Message event) {
        return event; // 直接返回传入的Message对象
    }
}