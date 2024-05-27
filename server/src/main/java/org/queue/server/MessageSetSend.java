package org.queue.server;


import io.netty.channel.socket.SocketChannel;
import org.queue.common.ErrorMapping;
import org.queue.message.MessageSet;
import org.queue.network.Send;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


public class MessageSetSend extends Send {

    private final Logger logger = LoggerFactory.getLogger(MessageSetSend.class);

    private MessageSet messages;
    private int errorCode;
    private long sent; // 已发送字节数
    private long size; // 消息集合的总大小（字节）
    private ByteBuffer header; // 消息集合的头部信息
    private boolean complete; // 消息发送是否完成的标志

    public MessageSetSend(MessageSet messages, int errorCode) {
        this.messages = messages;
        this.errorCode = errorCode;
        this.sent = 0;
        this.size = messages.sizeInBytes();
        // 初始化头部信息的ByteBuffer，大小为6字节（4字节大小 + 2字节错误码）
        this.header = ByteBuffer.allocate(6);
        // 设置头部信息：消息大小（转换为Int）和错误码（转换为Short）
        header.putInt((int)(size + 2));
        header.putShort((short)errorCode);
        header.rewind(); // 重置缓冲区的位置为起始位置
        this.complete = false;
    }

    // 辅助构造函数，当没有错误时使用默认错误码
    public MessageSetSend(MessageSet messages) {
        this(messages, ErrorMapping.NoError);
    }

    // 将消息集合写入到WritableByteChannel
    public int writeTo(WritableByteChannel channel) throws IOException {
        expectIncomplete();
        int written = 0;
        // 如果头部信息还有剩余未写入，则写入头部信息
        while (header.hasRemaining()) {
            written += channel.write(header);
        }
        // 如果头部信息已经完全写入，则写入消息内容
        if (!header.hasRemaining()) {
            long fileBytesSent = messages.writeTo(channel, sent, size - sent);
            written += (int)fileBytesSent;
            sent += fileBytesSent;
        }

        // 如果日志记录器启用了trace级别，记录发送的字节数和目标地址
        if (logger.isTraceEnabled()) {
            if (channel instanceof SocketChannel) {
                SocketChannel socketChannel = (SocketChannel)channel;
                logger.trace(sent + " bytes written to " + socketChannel.remoteAddress().toString() +
                        " expecting to send " + size + " bytes");
            }
        }

        // 如果已发送的字节数达到或超过总大小，则设置完成标志为true
        if (sent >= size) {
            complete = true;
        }
        return written;
    }
    /**
     * 抽象方法，用于检查传输是否已经完成。
     * 需要在子类中具体实现。
     *
     * @return 如果传输完成返回true，否则返回false。
     */
    @Override
    public boolean complete() {
        return this.complete ;
    }
    // 检查消息是否已经完全发送
    public void expectIncomplete() {
        if (complete()) {
            throw new IllegalStateException("Message set has already been completely sent.");
        }
    }

    // 返回需要发送的总大小，包括头部和消息内容
    public int sendSize() {
        return (int)(size + header.capacity());
    }
}