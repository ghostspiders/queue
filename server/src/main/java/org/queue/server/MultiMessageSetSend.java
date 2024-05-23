package org.queue.server;

import org.queue.network.MultiSend;

import java.nio.ByteBuffer;
import java.util.List;

public class MultiMessageSetSend extends MultiSend {
    private ByteBuffer buffer;
    private int allMessageSetSize;
    private int expectedBytesToWrite;

    public MultiMessageSetSend(List<MessageSetSend> sets) {
        // 创建一个ByteBufferSend对象，并将其添加到sets列表的开头
        super(sets);
        ByteBufferSend bufferSend = new ByteBufferSend(6);
        sends.add(0, bufferSend); // 将bufferSend添加到列表的开头
        this.buffer = bufferSend.getBuffer();
        // 计算所有MessageSetSend对象的总大小
        this.allMessageSetSize = calculateTotalSize(sets);
        // 计算预期需要写入的总字节数，包括头部信息（4字节消息集合大小 + 2字节保留字段）
        this.expectedBytesToWrite = 4 + 2 + allMessageSetSize;
        // 设置头部信息：消息集合大小（不包括头部自身的6字节）
        buffer.putInt(expectedBytesToWrite);
        // 保留字段，目前设置为0
        buffer.putShort((short) 0);
        // 重置缓冲区的位置为起始位置
        buffer.rewind();
    }

    // 辅助方法，用于计算所有MessageSetSend对象的总大小
    private int calculateTotalSize(List<MessageSetSend> sets) {
        int totalSize = 0;
        for (MessageSetSend set : sets) {
            totalSize += set.sendSize();
        }
        return totalSize;
    }
}
