package org.queue.api;

import org.queue.common.ErrorMapping;
import org.queue.network.Send;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.io.IOException;

// 假设ErrorMapping和OffsetRequest类已经定义在其他地方
public class OffsetArraySend extends Send {
    private long[] offsets; // 存储偏移量的数组
    private int size; // 数据包的总大小
    private ByteBuffer header; // 存储数据包头部信息
    private ByteBuffer contentBuffer; // 存储序列化后的偏移量数组
    private boolean complete; // 标记发送任务是否完成

    // 构造函数，接收一个偏移量数组
    public OffsetArraySend(long[] offsets) {
        this.offsets = offsets;
        this.size = calculateSize(); // 计算数据包大小
        this.header = ByteBuffer.allocate(6); // 分配头部缓冲区
        this.contentBuffer = OffsetRequest.serializeOffsetArray(offsets); // 序列化偏移量数组
        this.complete = false; // 初始化完成状态为未完成
        initializeHeader(); // 初始化头部信息
    }

    // 计算数据包大小
    private int calculateSize() {
        int calculatedSize = 4; // 初始大小为4字节
        for (long offset : offsets) { // 遍历偏移量数组
            calculatedSize += 8; // 每个偏移量占用8字节
        }
        return calculatedSize;
    }

    // 初始化头部信息
    private void initializeHeader() {
        header.putInt(size + 2); // 设置数据包大小
        header.putShort((short) ErrorMapping.NoError); // 设置错误代码为无错误
        header.rewind(); // 重置缓冲区位置
    }

    // 将数据写入到WritableByteChannel
    public int writeTo(WritableByteChannel channel) throws IOException {
        if (!complete) { // 确保发送任务未完成
            int written = 0; // 初始化已写入字节数

            // 如果头部还有剩余数据，则写入头部数据
            while (header.hasRemaining()) {
                written += channel.write(header);
            }

            // 如果头部数据已完全写入且内容缓冲区还有剩余数据，则写入内容数据
            if (header.position() == header.limit() && contentBuffer.hasRemaining()) {
                written += channel.write(contentBuffer);
            }

            // 如果内容缓冲区没有剩余数据，则设置完成状态为true
            if (!contentBuffer.hasRemaining()) {
                complete = true;
            }

            return written; // 返回已写入的字节数
        }
        return 0; // 如果发送任务已完成，则返回0
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
}