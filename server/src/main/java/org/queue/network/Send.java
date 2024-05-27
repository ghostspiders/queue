package org.queue.network;

/**
 * @author gaoyvfeng
 * @ClassName Send
 * @description:
 * @datetime 2024年 05月 22日 10:55
 * @version: 1.0
 */
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * 抽象类Send，继承自Transmission，用于定义发送数据的行为。
 */
public abstract class Send extends Transmission {

    /**
     * 抽象方法，用于将数据写入给定的WritableByteChannel。
     *
     * @param channel 要写入的通道。
     * @return 写入的字节数。
     */
    public abstract int writeTo(WritableByteChannel channel) throws IOException;

    /**
     * 具体方法，用于完全写入数据到给定的WritableByteChannel，直到传输完成。
     *
     * @param channel 要写入的通道。
     * @return 写入的总字节数。
     */
    public int writeCompletely(WritableByteChannel channel) throws IOException {
        int totalWritten = 0; // 用于记录总共写入的字节数
        while (!complete()) { // 循环直到传输完成
            int written = writeTo(channel); // 写入数据到通道
            totalWritten += written; // 累加写入的字节数
            if (logger.isTraceEnabled()) {
                logger.trace("{} bytes written.", written); // 如果启用trace级别日志，记录写入的字节数
            }
        }
        return totalWritten; // 返回总共写入的字节数
    }
}
