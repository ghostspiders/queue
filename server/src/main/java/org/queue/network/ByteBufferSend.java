package org.queue.network;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.io.IOException;

public class ByteBufferSend extends Send {
    private ByteBuffer buffer; // ByteBuffer用于数据发送
    private boolean complete; // 发送是否完成的标志

    public ByteBufferSend(ByteBuffer buffer) {
        this.buffer = buffer;
        this.complete = false;
    }

    // 辅助构造函数，允许通过指定大小创建ByteBuffer
    public ByteBufferSend(int size) {
        this(ByteBuffer.allocate(size));
    }

    // 将ByteBuffer写入WritableByteChannel，并返回写入的字节数
    public int writeTo(WritableByteChannel channel) throws IOException {
        expectIncomplete(); // 确保发送还未完成
        int written = channel.write(buffer); // 写入ByteBuffer到通道，并获取写入的字节数
        if (!buffer.hasRemaining()) complete = true; // 如果ByteBuffer中没有剩余数据，则设置完成标志
        return written; // 返回写入的总字节数
    }

    // 检查发送是否已完成，如果已完成，则抛出异常
    public void expectIncomplete() {
        if (complete()) {
            throw new IllegalStateException("ByteBufferSend is complete");
        }
    }
    @Override
    public boolean complete() {
        return this.complete ;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }
}
