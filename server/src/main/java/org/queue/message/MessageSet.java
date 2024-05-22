package org.queue.message;

/**
 * @author gaoyvfeng
 * @ClassName MessageSet
 * @description:
 * @datetime 2024年 05月 22日 11:19
 * @version: 1.0
 */

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

/**
 * 消息集合。消息集合具有固定的序列化形式，尽管字节的容器可以是内存中的或磁盘上的。
 */
public abstract class MessageSet implements Iterable<MessageAndOffset> {
    /**
     * 日志条目的固定额外开销字节数。
     */
    public static final int LogOverhead = 4;

    /**
     * 空的消息集合，使用ByteBuffer.allocate(0)分配大小为0的ByteBuffer。
     */
    public static final MessageSet Empty = new ByteBufferMessageSet(ByteBuffer.allocate(0));

    /**
     * 计算给定消息集合的总大小。
     *
     * @param messages 消息的Iterable集合。
     * @return 消息集合的总大小。
     */
    public static int messageSetSize(Iterable<Message> messages) {
        int size = 0;
        for (Message message : messages) {
            size += entrySize(message);
        }
        return size;
    }

    /**
     * 计算给定Java List中消息集合的总大小。
     *
     * @param messages Java List中的消息集合。
     * @return 消息集合的总大小。
     */
    public static int messageSetSize(List<Message> messages) {
        int size = 0;
        for (Message message : messages) {
            size += entrySize(message);
        }
        return size;
    }

    /**
     * 计算消息集合中单个条目的大小。
     *
     * @param message 消息对象。
     * @return 单个条目的大小。
     */
    public static int entrySize(Message message) {
        return LogOverhead + message.size();
    }
    /**
     * 将此集合中的消息写入给定的通道，从给定的偏移量开始。
     * 可能写入的字节少于完整数量，但不会超过maxSize。返回写入的字节数。
     *
     * @param channel 要写入的WritableByteChannel通道。
     * @param offset  开始写入的偏移量。
     * @param maxSize 可以写入的最大字节数。
     * @return 写入的字节数。
     */
    public abstract long writeTo(WritableByteChannel channel, long offset, long maxSize);

    /**
     * 提供此集合中消息的迭代器。
     *
     * @return 消息迭代器。
     */
    @Override
    public abstract Iterator<MessageAndOffset> iterator();

    /**
     * 返回此消息集合的总字节大小。
     *
     * @return 消息集合的总字节大小。
     */
    public abstract long sizeInBytes();

    /**
     * 验证集合中所有消息的校验和。如果任何消息的校验和与有效载荷不匹配，则抛出InvalidMessageException。
     */
    public void validate() throws InvalidMessageException {
        Iterator<MessageAndOffset> iterator = this.iterator();
        while (iterator.hasNext()) {
            MessageAndOffset messageAndOffset = iterator.next();
            if (!messageAndOffset.getMessage().isValid()) {
                throw new InvalidMessageException();
            }
        }
    }
}
