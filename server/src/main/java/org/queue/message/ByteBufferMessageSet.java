package org.queue.message;

import org.queue.common.ErrorMapping;
import org.queue.common.InvalidMessageSizeException;
import org.queue.utils.IteratorTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.List;

/**
 * 存储在字节缓冲区中的消息序列。
 * <p>
 * ByteBufferMessageSet有两种创建方式：
 * 选项1：从已经包含序列化消息集的ByteBuffer。消费者将使用此方法。
 * 选项2：提供消息列表以及与序列化格式相关的指令。生产者将使用此方法。
 */
public class ByteBufferMessageSet extends MessageSet {
    private static final Logger logger = LoggerFactory.getLogger(ByteBufferMessageSet.class);
    private ByteBuffer buffer;
    private long initialOffset;
    private int errorCode;

    /**
     * 浅层有效的字节计数，用于跟踪浅层验证过程中的字节数。
     * 在Scala中初始化为-1L表示初始未定义状态。
     */
    private long shallowValidByteCount = -1L;

    /**
     * 深层有效的字节计数，用于跟踪深层验证过程中的字节数。
     * 在Scala中初始化为-1L表示初始未定义状态。
     */
    private long deepValidByteCount = -1L;

    // 构造函数，从ByteBuffer创建
    public ByteBufferMessageSet(ByteBuffer buffer, long initialOffset, int errorCode) {
        this.buffer = buffer;
        this.initialOffset = initialOffset;
        this.errorCode = errorCode;
    }
    public ByteBufferMessageSet(ByteBuffer buffer) {
        this.buffer = buffer;
        this.initialOffset = 0L;
        this.errorCode = ErrorMapping.NoError;
    }
    public ByteBufferMessageSet(CompressionCodec compressionCodec, List<Message>  messages) {
        // 根据compressionCodec的类型，初始化buffer
        ByteBuffer byteBuffer;
        if (compressionCodec == new NoCompressionCodec()) {
            // 无压缩情况
            byteBuffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages));
            for (Message message : messages) {
                message.serializeTo(byteBuffer);
            }
            byteBuffer.rewind();
        } else {
            // 压缩情况
            Message message = CompressionUtils.compress(messages, compressionCodec);
            byteBuffer = ByteBuffer.allocate(message.serializedSize());
            message.serializeTo(byteBuffer);
            byteBuffer.rewind();
        }
        this.initialOffset = 0L;
        this.errorCode = ErrorMapping.NoError;
    }

    // 构造函数，从消息列表创建（不压缩）
    public ByteBufferMessageSet(List<Message> messages) {
        this(new NoCompressionCodec(), messages);
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

    // 序列化消息集
    public ByteBuffer serialized() {
        return buffer;
    }

    // 获取有效字节数
    public long validBytes() throws Throwable {
        return deepValidBytes();
    }

    // 获取浅层有效字节数
    public long shallowValidBytes() throws Throwable {
        if (shallowValidByteCount < 0) {
            Iterator<MessageAndOffset> iter = deepIterator();
            while (iter.hasNext()) {
                MessageAndOffset messageAndOffset = iter.next();
                shallowValidByteCount = messageAndOffset.getOffset();
            }
        }
        return shallowValidByteCount - initialOffset;
    }

    // 获取深层有效字节数
    public long deepValidBytes() throws Throwable {
        if (deepValidByteCount < 0) {
            Iterator<MessageAndOffset> iter = deepIterator();
            while (iter.hasNext())
                iter.next();
        }
        return deepValidByteCount;
    }

    // 将消息集写入给定通道
    public long writeTo(WritableByteChannel channel, long offset, long size) throws IOException {
        ByteBuffer duplicate = buffer.duplicate();
        return channel.write(duplicate);
    }

    // 获取迭代器
    @Override
    public Iterator<MessageAndOffset> iterator() {
        try {
            return deepIterator();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    // 深层迭代器
    protected Iterator<MessageAndOffset> deepIterator() throws Throwable {
        ErrorMapping.maybeThrowException(errorCode);
        return new IteratorTemplate<MessageAndOffset>() {
            ByteBuffer topIter = buffer.slice();
            long currValidBytes = initialOffset;
            Iterator<MessageAndOffset> innerIter = null;
            long lastMessageSize = 0;

            boolean innerDone() {
                return innerIter == null || !innerIter.hasNext();
            }

            MessageAndOffset makeNextOuter() throws Throwable {
                if (topIter.remaining() < 4) {
                    deepValidByteCount = currValidBytes;
                    return allDone();
                }
                int size = topIter.getInt();
                lastMessageSize = size;

                if (logger.isTraceEnabled()) {
                    logger.trace("Remaining bytes in iterator = " + topIter.remaining());
                    logger.trace("size of data = " + size);
                }
                if (size < 0 || topIter.remaining() < size) {
                    deepValidByteCount = currValidBytes;
                    if (currValidBytes == 0 || size < 0)
                        throw new InvalidMessageSizeException("invalid message size: " + size + " only received bytes: " + topIter.remaining() +
                                " at " + currValidBytes + " possible causes (1) a single message larger than the fetch size; (2) log corruption");
                    return allDone();
                }
                ByteBuffer message = topIter.slice();
                message.limit(size);
                topIter.position(topIter.position() + size);
                Message newMessage = new Message(message);
                if (new NoCompressionCodec().equals(newMessage.compressionCodec())) {
                    if (logger.isDebugEnabled())
                        logger.debug("Message is uncompressed. Valid byte count = " + currValidBytes);
                    innerIter = null;
                    currValidBytes += 4 + size;
                    return new MessageAndOffset(newMessage, currValidBytes);
                }
                if (logger.isDebugEnabled())
                    logger.debug("Message is compressed. Valid byte count = " + currValidBytes);
                innerIter = CompressionUtils.decompress(newMessage).deepIterator();
                return makeNext();
            }

            @Override
            public MessageAndOffset makeNext() throws Throwable {
                if (logger.isDebugEnabled())
                    logger.debug("makeNext() in deepIterator: innerDone = " + innerDone());
                if (innerDone()) {
                    return makeNextOuter();
                } else {
                    MessageAndOffset messageAndOffset = innerIter.next();
                    if (!innerIter.hasNext())
                        currValidBytes += 4 + lastMessageSize;
                    return new MessageAndOffset(messageAndOffset.getMessage(), currValidBytes);
                }
            }
        };
    }

    // 获取字节大小
    public long sizeInBytes() {
        return buffer.limit();
    }

    // 转换为字符串
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ByteBufferMessageSet(");
        Iterator<MessageAndOffset> iter = this.iterator();
        while (iter.hasNext()) {
            MessageAndOffset message = iter.next();
            builder.append(message.toString());
            if (iter.hasNext()) {
                builder.append(", ");
            }
        }
        builder.append(")");
        return builder.toString();
    }

    // 检查是否等于其他对象
    @Override
    public boolean equals(Object other) {
        if (other instanceof ByteBufferMessageSet) {
            ByteBufferMessageSet that = (ByteBufferMessageSet) other;
            return this.canEqual(that) &&
                    this.errorCode == that.errorCode &&
                    this.buffer.equals(that.buffer) &&
                    this.initialOffset == that.initialOffset;
        }
        return false;
    }

    // 检查是否可以与其他对象比较
    public boolean canEqual(Object other) {
        return other instanceof ByteBufferMessageSet;
    }

    // 计算hashCode
    @Override
    public int hashCode() {
        return (31 + (17 * errorCode) + buffer.hashCode() + (int) initialOffset);
    }
}