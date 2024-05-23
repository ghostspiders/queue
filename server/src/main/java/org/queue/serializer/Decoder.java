package org.queue.serializer;

import org.queue.message.Message;

/**
 * @author gaoyvfeng
 * @ClassName Decoder
 * @description:
 * @datetime 2024年 05月 23日 15:43
 * @version: 1.0
 */
interface Decoder<T> {
    T toEvent(Message message);
}