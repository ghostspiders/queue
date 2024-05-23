package org.queue.network;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.io.IOException;

public class BoundedByteBufferReceive {

    private final int maxSize; // 最大消息大小
    private ByteBuffer sizeBuffer; // 用于存储消息大小的缓冲区
    private ByteBuffer contentBuffer; // 用于存储消息内容的缓冲区
    private boolean complete; // 标记消息是否完整接收

    // 构造函数，设置最大消息大小
    public BoundedByteBufferReceive(int maxSize) {
        this.maxSize = maxSize;
        sizeBuffer = ByteBuffer.allocate(4); // 分配4字节用于存储消息大小
    }

    // 无参构造函数，使用最大整数值作为最大消息大小
    public BoundedByteBufferReceive() {
        this(Integer.MAX_VALUE);
    }

    /**
     * 获取消息内容的缓冲区
     * 确保消息已经完整接收
     */
    public ByteBuffer getBuffer() {
        expectComplete();
        return contentBuffer;
    }

    /**
     * 从给定的通道读取数据
     * @param channel 可读字节通道
     * @return 读取的字节数
     * @throws IOException 如果读取过程中发生IO异常
     */
    public int readFrom(ReadableByteChannel channel) throws IOException {
        expectIncomplete();
        int read = 0;

        // 是否已经读取了请求的大小
        if (sizeBuffer.remaining() > 0) {
            read += channel.read(sizeBuffer);
        }

        // 是否已经分配了请求缓冲区
        if (contentBuffer == null && !sizeBuffer.hasRemaining()) {
            sizeBuffer.rewind(); // 重置position指针到起始位置
            int size = sizeBuffer.getInt(); // 获取消息大小
            if (size <= 0 || size > maxSize) {
                throw new IllegalArgumentException(size + " is not a valid message size");
            }
            contentBuffer = ByteBuffer.allocate(size); // 分配缓冲区
        }

        // 如果已经分配了缓冲区，则读取数据到缓冲区中
        if (contentBuffer != null) {
            read += channel.read(contentBuffer);
            // 如果缓冲区已满，消息完整接收
            if (!contentBuffer.hasRemaining()) {
                contentBuffer.rewind(); // 重置position指针到起始位置
                complete = true;
            }
        }

        return read;
    }

    /**
     * 确保消息已经完整接收
     * 如果消息不完整，抛出异常
     */
    private void expectComplete() {
        if (!complete) {
            throw new IllegalStateException("Message is not complete");
        }
    }

    /**
     * 确保消息尚未完整接收
     * 如果消息已经完整接收，抛出异常
     */
    private void expectIncomplete() {
        if (complete) {
            throw new IllegalStateException("Message has already been completed");
        }
    }
}