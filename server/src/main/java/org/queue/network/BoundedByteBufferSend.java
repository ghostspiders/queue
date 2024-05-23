package org.queue.network;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.io.IOException;

public class BoundedByteBufferSend extends Send {
    private ByteBuffer buffer; // 用于数据发送的ByteBuffer
    private ByteBuffer sizeBuffer; // 用于存储消息长度的ByteBuffer
    private boolean complete; // 发送是否完成的标志

    public BoundedByteBufferSend(ByteBuffer buffer) {
        this.buffer = buffer;
        this.sizeBuffer = ByteBuffer.allocate(4);
        this.complete = false;
        sizeBuffer.putInt(buffer.limit());
        sizeBuffer.rewind();
    }

    public BoundedByteBufferSend(int size) {
        this(ByteBuffer.allocate(size));
    }

    public BoundedByteBufferSend(Request request) {
        this(request.sizeInBytes() + 2);
        buffer.putShort((short) request.getId());
        request.writeTo(buffer);
        buffer.rewind();
    }

    public int writeTo(WritableByteChannel channel) throws IOException {
        expectIncomplete(); // 确保发送还未完成
        int written = 0;
        // 如果sizeBuffer还有剩余未发送，则尝试发送sizeBuffer
        while(sizeBuffer.hasRemaining()) {
            written += channel.write(sizeBuffer);
        }
        // 如果sizeBuffer已经发送完毕且buffer还有剩余数据，则尝试发送buffer
        while(buffer.hasRemaining()) {
            written += channel.write(buffer);
        }
        // 如果buffer发送完毕，则设置完成标志
        if(!buffer.hasRemaining()) {
            complete = true;
        }

        return written; // 返回写入的总字节数
    }

    public void expectIncomplete() {
        if (complete()) {
            throw new IllegalStateException("BoundedByteBufferSend is complete");
        }
    }
    @Override
    public boolean complete() {
        return this.complete ;
    }
}