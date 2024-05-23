package org.queue.serializer;

import org.queue.message.Message;

import java.nio.ByteBuffer;

/**
 * @author gaoyvfeng
 * @ClassName StringDecoder
 * @description:
 * @datetime 2024年 05月 23日 15:44
 * @version: 1.0
 */
// 实现Decoder接口的StringDecoder类
class StringDecoder implements Decoder<String> {
    @Override
    public String toEvent(Message message) {
        ByteBuffer buf = message.payload(); // 获取Message的payload
        byte[] arr = new byte[buf.remaining()];
        buf.get(arr); // 将payload数据读入数组
        return new String(arr); // 将字节数组转换成字符串
    }
}
