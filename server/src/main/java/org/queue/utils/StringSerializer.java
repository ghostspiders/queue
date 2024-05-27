package org.queue.utils;

import org.I0Itec.zkclient.serialize.ZkSerializer;
import java.io.UnsupportedEncodingException;

/**
 * 字符串序列化器，实现了ZkSerializer接口，用于ZooKeeper的序列化和反序列化操作。
 */
public class StringSerializer implements ZkSerializer {

    /**
     * 序列化给定的数据对象到字节数组。
     * @param data 要序列化的对象，期望为String类型。
     * @return 返回序列化后的字节数组。
     */
    @Override
    public byte[] serialize(Object data){
        // 确保传入的数据是String类型
        if (data instanceof String) {
            try {
                return ((String) data).getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new IllegalArgumentException("Data object is not a String: " + data.getClass().getName());
        }
    }

    /**
     * 反序列化给定的字节数组到一个对象。
     * @param bytes 要反序列化的字节数组。
     * @return 返回反序列化后的对象，类型为String。
     */
    @Override
    public Object deserialize(byte[] bytes) {
        if (bytes == null) {
            return null;
        } else {
            try {
                return new String(bytes, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                // UTF-8编码通常不会抛出此异常，如果抛出则说明环境有问题
                throw new RuntimeException("UTF-8 encoding not supported", e);
            }
        }
    }
}