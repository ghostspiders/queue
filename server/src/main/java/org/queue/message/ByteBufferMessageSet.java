package org.queue.message;

/**
 * @author gaoyvfeng
 * @ClassName ByteBufferMessageSet
 * @description:
 * @datetime 2024年 05月 22日 14:30
 * @version: 1.0
 */
import org.queue.common.ErrorMapping;
import org.queue.common.InvalidMessageSizeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 存储在字节缓冲区（ByteBuffer）中的一系列消息
 * <p>
 * 创建ByteBufferMessageSet有两种方式：
 * 方式1：从一个已经包含序列化消息集合的ByteBuffer。消费者将使用此方法。
 * 方式2：给它一个消息列表以及与序列化格式相关的指令。生产者将使用此方法。
 */
public class ByteBufferMessageSet extends MessageSet {
    private  final Logger logger = LoggerFactory.getLogger(ByteBufferMessageSet.class);

    private ByteBuffer buffer;
    private long initialOffset;
    private int errorCode;

    // 构造函数，从ByteBuffer创建ByteBufferMessageSet
    public ByteBufferMessageSet(ByteBuffer buffer, long initialOffset, int errorCode) {
        this.buffer = buffer;
        this.initialOffset = initialOffset;
        this.errorCode = errorCode;
    }

    // 构造函数，使用压缩编解码器和消息序列创建ByteBufferMessageSet
    public ByteBufferMessageSet(CompressionCodec compressionCodec, Message... messages) {
        this(createByteBufferFromMessages(compressionCodec, messages), 0L, ErrorMapping.NoError);
    }

    // 构造函数，使用无压缩编解码器和消息序列创建ByteBufferMessageSet
    public ByteBufferMessageSet(Message... messages) {
        this(NoCompressionCodec.INSTANCE, messages);
    }

    // 获取初始偏移量
    public long getInitialOffset() {
        return initialOffset;
    }

    // 获取ByteBuffer
    public ByteBuffer getBuffer() {
        return buffer;
    }

    // 获取错误代码
    public int getErrorCode() {
        return errorCode;
    }

    // 将消息集合写入给定的WritableByteChannel
    @Override
    public long writeTo(WritableByteChannel channel, long offset, long size) {
        ByteBuffer duplicate = buffer.duplicate();
        long written = 0;
        while (duplicate.hasRemaining() && written < size) {
            written += channel.write(duplicate);
        }
        return written;
    }

    // 提供迭代器
    @Override
    public Iterator<MessageAndOffset> iterator() {
        return deepIterator();
    }

    // 私有方法，返回深层迭代器
    private Iterator<MessageAndOffset> deepIterator() {
        ErrorMapping.maybeThrowException(errorCode);
        // 这里需要实现IteratorTemplate的具体逻辑
        // ...
    }

    // 返回ByteBufferMessageSet的字节大小
    public long sizeInBytes() {
        return buffer.limit();
    }

    // 重写toString方法
    @Override
    public String toString() {
        // 实现细节
        return "ByteBufferMessageSet{...}";
    }

    // 重写equals方法
    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        ByteBufferMessageSet that = (ByteBufferMessageSet) other;
        // 实现细节
        return true; // 示例，需要具体实现
    }

    // 重写hashCode方法
    @Override
    public int hashCode() {
        // 实现细节
        return 0; // 示例，需要具体实现
    }

    // 创建ByteBuffer的辅助方法
    private  ByteBuffer createByteBufferFromMessages(CompressionCodec codec, Message... messages) {
        // 根据codec和messages创建ByteBuffer
        // ...
        return null;
    }
}