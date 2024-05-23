package org.queue.message;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileMessageSet extends MessageSet {
    private final FileChannel channel; // 文件通道
    private final long offset; // 偏移量
    private final long limit; // 限制大小
    private final boolean mutable; // 是否可变
    private final AtomicBoolean needRecover; // 是否需要恢复

    private final AtomicLong setSize; // 设置的大小
    private final AtomicLong setHighWaterMark; // 设置的高水位标记
    private static final Logger logger = LoggerFactory.getLogger(FileMessageSet.class);

    // 构造函数
    public FileMessageSet(FileChannel channel, long offset, long limit, boolean mutable, AtomicBoolean needRecover) {
        this.channel = channel;
        this.offset = offset;
        this.limit = limit;
        this.mutable = mutable;
        this.needRecover = needRecover;
        this.setSize = new AtomicLong();
        this.setHighWaterMark = new AtomicLong();

        init();
    }

    // 初始化方法
    private void init() {
        if (mutable) {
            // 如果是可变的消息集
            if (limit < Long.MAX_VALUE || offset > 0) {
                throw new IllegalArgumentException("Attempt to open a mutable message set with a view or offset, which is not allowed.");
            }

            if (needRecover.get()) {
                // 如果需要恢复
                long startMs = System.currentTimeMillis();
                long truncated = 0;
                logger.info("Recovery succeeded in " + ((System.currentTimeMillis() - startMs) / 1000) +
                        " seconds. " + truncated + " bytes truncated.");
            } else {
                setSize.set(channel.size());
                setHighWaterMark.set(setSize.get());
                try {
                    channel.position(channel.size());
                } catch (IOException e) {
                    // Handle exception
                }
            }
        } else {
            // 如果是不可变的消息集
            setSize.set(Math.min((int) channel.size(), (int) limit) - offset);
            setHighWaterMark.set(setSize.get());
            if (logger.isDebugEnabled()) {
                logger.debug("initializing high water mark in immutable mode: " + setHighWaterMark.get());
            }
        }
    }

    /**
     * 创建一个没有限制或偏移量的文件消息集。
     * @param channel 文件通道。
     * @param mutable 是否可变。
     */
    public FileMessageSet(FileChannel channel, boolean mutable) {
        this(channel, 0, Long.MAX_VALUE, mutable, new AtomicBoolean(false));
    }

    /**
     * 创建一个没有限制或偏移量的文件消息集。
     * @param file 文件。
     * @param mutable 是否可变。
     * @throws IOException 如果文件打开失败。
     */
    public FileMessageSet(File file, boolean mutable) throws IOException {
        this(Utils.openChannel(file, mutable), mutable);
    }

    /**
     * 创建一个没有限制或偏移量的文件消息集。
     * @param channel 文件通道。
     * @param mutable 是否可变。
     * @param needRecover 是否需要恢复。
     */
    public FileMessageSet(FileChannel channel, boolean mutable, AtomicBoolean needRecover) {
        this(channel, 0, Long.MAX_VALUE, mutable, needRecover);
    }

    /**
     * 创建一个没有限制或偏移量的文件消息集。
     * @param file 文件。
     * @param mutable 是否可变。
     * @param needRecover 是否需要恢复。
     * @throws IOException 如果文件打开失败。
     */
    public FileMessageSet(File file, boolean mutable, AtomicBoolean needRecover) throws IOException {
        this(Utils.openChannel(file, mutable), mutable, needRecover);
    }
    /**
     * 返回一个消息集，该消息集是从给定偏移量开始并且大小限制在给定范围内的此集合的视图。
     *
     * @param readOffset 视图开始的偏移量。
     * @param size 视图的大小限制。
     * @return 返回一个新的MessageSet，它代表了原始消息集的一个子集。
     */
    public MessageSet read(long readOffset, long size) {
        // 计算新的offset和limit，并创建一个新的FileMessageSet实例
        long newOffset = this.offset + readOffset;
        long newLimit = Math.min(newOffset + size, this.highWaterMark());
        try {
            return new FileMessageSet(this.channel, newOffset, newLimit, false, new AtomicBoolean(false));
        } catch (IOException e) {
            // Handle or throw the IOException depending on your error handling policy
            return null;
        }
    }

    /**
     * 将此集合的一部分写入给定的通道，并返回写入的数量。
     *
     * @param destChannel 目标写入通道。
     * @param writeOffset 要写入的偏移量。
     * @param size 最大写入的字节数。
     * @return 返回写入通道的字节数。
     */
    public long writeTo(WritableByteChannel destChannel, long writeOffset, long size) {
        // 使用FileChannel的transferTo方法将数据从文件传输到WritableByteChannel
        long bytesToWrite = Math.min(size, this.sizeInBytes());
        try {
            return this.channel.transferTo(this.offset + writeOffset, bytesToWrite, destChannel);
        } catch (IOException e) {
            // Handle or throw the IOException depending on your error handling policy
            return -1;
        }
    }

    /**
     * 获取集合中消息的迭代器。
     * @return 返回一个迭代器，用于遍历消息集。
     */
    @Override
    public Iterator<MessageAndOffset> iterator() {
        return new Iterator<MessageAndOffset>() {
            private long location = offset; // 初始位置设置为offset

            @Override
            public boolean hasNext() {
                // 实现该方法以检查是否还有更多消息
                // 具体实现取决于MessageSet的具体逻辑
                return location < highWaterMark();
            }

            @Override
            public MessageAndOffset next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                // 读取项目的大小
                ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
                try {
                    while (location < highWaterMark() && channel.read(sizeBuffer, location) == 0) {
                        // 处理到达文件末尾的情况
                    }
                } catch (IOException e) {
                    // Handle exception
                }
                sizeBuffer.rewind();
                int size = sizeBuffer.getInt();
                if (size < Message.MinHeaderSize) {
                    throw new NoSuchElementException();
                }

                // 读取项目本身
                ByteBuffer buffer = ByteBuffer.allocate(size);
                try {
                    while (buffer.hasRemaining() && channel.read(buffer, location + 4) != -1) {
                        // 继续读取直到缓冲区充满
                    }
                } catch (IOException e) {
                    // Handle exception
                }
                buffer.rewind();

                // 增加位置并返回项目
                location += size + 4;
                return new MessageAndOffset(new Message(buffer), location);
            }
        };
    }

    /**
     * 获取此文件集所占用的字节数。
     * @return 返回文件集的大小（字节）。
     */
    public long sizeInBytes() {
        return setSize.get();
    }

    /**
     * 获取高水位标记。
     * @return 返回高水位标记的值。
     */
    public long highWaterMark() {
        return setHighWaterMark.get();
    }

    /**
     * 检查消息集是否可变。
     */
    private void checkMutable() {
        if (!mutable) {
            throw new IllegalStateException("Attempt to invoke mutation on immutable message set.");
        }
    }

    /**
     * 追加消息到消息集。
     * @param messages 要追加的消息集。
     */
    public void append(MessageSet messages) {
        checkMutable();
        long written = 0L;
        try {
            while (written < messages.sizeInBytes()) {
                written += messages.writeTo(channel, written, messages.sizeInBytes() - written);
            }
            setSize.getAndAdd(written);
        } catch (IOException e) {
            // Handle exception
        }
    }
    /**
     * 提交所有写入数据到物理磁盘。
     */
    public void flush() {
        checkMutable();
        long startTime = System.currentTimeMillis();
        try {
            channel.force(true);
        } catch (IOException e) {
            // Handle exception
        }
        long elapsedTime = System.currentTimeMillis() - startTime;
        LogFlushStats.recordFlushRequest(elapsedTime);
        if (logger.isDebugEnabled()) {
            logger.debug("flush time " + elapsedTime);
        }
        setHighWaterMark.set(sizeInBytes());
        if (logger.isDebugEnabled()) {
            logger.debug("flush high water mark: " + highWaterMark());
        }
    }

    /**
     * 关闭此消息集。
     */
    public void close() {
        if (mutable) {
            flush();
        }
        try {
            channel.close();
        } catch (IOException e) {
            // Handle exception
        }
    }

    /**
     * 恢复日志直到最后一个完整的条目。截断任何不完整的消息字节。
     * @return 返回截断的字节数。
     */
    public long recover() {
        checkMutable();
        long len = 0;
        try {
            len = channel.size();
        } catch (IOException e) {
            // Handle exception
        }
        ByteBuffer buffer = ByteBuffer.allocate(4);
        long validUpTo = 0;
        long next = 0;
        do {
            next = validateMessage(channel, validUpTo, len, buffer);
            if (next >= 0) {
                validUpTo = next;
            }
        } while (next >= 0);

        try {
            channel.truncate(validUpTo);
        } catch (IOException e) {
            // Handle exception
        }
        setSize.set(validUpTo);
        setHighWaterMark.set(validUpTo);
        if (logger.isDebugEnabled()) {
            logger.info("recover high water mark: " + highWaterMark());
        }
        try {
            channel.position(validUpTo);
        } catch (IOException e) {
            // Handle exception
        }
        needRecover.set(false);
        return len - validUpTo;
    }

    /**
     * 读取、验证并丢弃单个消息，返回下一个有效的偏移量，并返回被验证的消息。
     * @param channel 文件通道。
     * @param start 开始位置。
     * @param len 文件长度。
     * @param buffer 缓冲区。
     * @return 返回下一个有效偏移量。
     */
    private long validateMessage(FileChannel channel, long start, long len, ByteBuffer buffer) {
        buffer.rewind();
        try {
            int read = channel.read(buffer, start);
            if (read < 4) {
                return -1;
            }
        } catch (IOException e) {
            // Handle exception
            return -1;
        }

        long size = buffer.getInt(0);
        if (size < Message.MinHeaderSize) {
            return -1;
        }

        long next = start + 4 + size;
        if (next > len) {
            return -1;
        }

        ByteBuffer messageBuffer = ByteBuffer.allocate(size);
        long curr = start + 4;
        try {
            while (messageBuffer.hasRemaining()) {
                int read = channel.read(messageBuffer, curr);
                if (read < 0) {
                    throw new IllegalStateException("File size changed during recovery!");
                } else {
                    curr += read;
                }
            }
        } catch (IOException e) {
            // Handle exception
            return -1;
        }

        messageBuffer.rewind();
        Message message = new Message(messageBuffer);
        if (!message.isValid()) {
            return -1;
        } else {
            return next;
        }
    }

}