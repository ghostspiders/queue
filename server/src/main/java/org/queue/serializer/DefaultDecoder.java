package org.queue.serializer;

import org.queue.message.Message;

/**
 * @author gaoyvfeng
 * @ClassName DefaultDecoder
 * @description:
 * @datetime 2024年 05月 23日 15:44
 * @version: 1.0
 */
// 实现Decoder接口的DefaultDecoder类
class DefaultDecoder implements Decoder<Message> {
    @Override
    public Message toEvent(Message message) {
        return message; // 直接返回传入的Message对象
    }
}