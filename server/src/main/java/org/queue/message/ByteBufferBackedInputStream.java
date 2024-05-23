package org.queue.message;

/**
 * @author gaoyvfeng
 * @ClassName ByteBufferBackedInputStream
 * @description:
 * @datetime 2024年 05月 23日 10:51
 * @version: 1.0
 */
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * ByteBufferBackedInputStream类提供对ByteBuffer的包装，使其可以像InputStream那样被读取。
 */
public class ByteBufferBackedInputStream extends InputStream {
    // 包装的ByteBuffer对象
    private ByteBuffer buffer;

    /**
     * 构造函数，接收一个ByteBuffer对象。
     * @param buffer 要包装的ByteBuffer对象。
     */
    public ByteBufferBackedInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * 读取单个字节数据。
     * @return 如果还有可读的数据则返回读取的字节，否则返回-1。
     */
    @Override
    public int read() {
        // 检查ByteBuffer中是否还有剩余数据
        if (buffer.hasRemaining()) {
            // 读取一个字节并将其转换为0到255之间的int值
            return buffer.get() & 0xFF;
        } else {
            // 如果没有剩余数据，返回-1
            return -1;
        }
    }

    /**
     * 读取一些字节数并将它们存储到数组中。
     * @param bytes 要读取到的字节数组。
     * @param off 数组中的偏移量。
     * @param len 要读取的最大字节数。
     * @return 实际读取的字节数，如果到达流的末尾则返回-1。
     */
    @Override
    public int read(byte[] bytes, int off, int len) {
        // 检查ByteBuffer中是否还有剩余数据
        if (buffer.hasRemaining()) {
            // 计算实际要读取的字节数，不超过请求的长度或ByteBuffer中的剩余长度
            int realLen = Math.min(len, buffer.remaining());
            // 从ByteBuffer中读取字节到数组中
            buffer.get(bytes, off, realLen);
            // 返回实际读取的字节数
            return realLen;
        } else {
            // 如果没有剩余数据，返回-1
            return -1;
        }
    }
}