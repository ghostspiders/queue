package org.queue.serializer;

import org.queue.message.Message;

/**
 * @author gaoyvfeng
 * @ClassName Encoder
 * @description:
 * @datetime 2024年 05月 23日 15:46
 * @version: 1.0
 */
public interface Encoder<T> {
    Message toMessage(T event);
}
