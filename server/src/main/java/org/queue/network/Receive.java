package org.queue.network;

/**
 * @author gaoyvfeng
 * @ClassName Receive
 * @description:
 * @datetime 2024年 05月 22日 10:54
 * @version: 1.0
 */
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * 抽象类Receive，继承自Transmission，用于定义接收数据的行为。
 */
public abstract class Receive extends Transmission {

    /**
     * 抽象方法，用于获取用于存储接收数据的ByteBuffer。
     *
     * @return ByteBuffer对象。
     */
    public abstract ByteBuffer getBuffer();

    /**
     * 抽象方法，用于从给定的ReadableByteChannel读取数据。
     *
     * @param channel 要读取的通道。
     * @return 读取的字节数。
     */
    public abstract int readFrom(ReadableByteChannel channel) throws IOException;

    /**
     * 具体方法，用于从给定的ReadableByteChannel完全读取数据，直到传输完成。
     *
     * @param channel 要读取的通道。
     * @return 读取的总字节数。
     */
    public int readCompletely(ReadableByteChannel channel) throws IOException {
        int totalRead = 0; // 用于记录总共读取的字节数
        while (!complete()) { // 循环直到传输完成
            int read = readFrom(channel); // 从通道读取数据
            totalRead += read; // 累加读取的字节数
            if (logger.isTraceEnabled()) {
                logger.trace("{} bytes read.", read); // 如果启用trace级别日志，记录读取的字节数
            }
        }
        return totalRead; // 返回总共读取的字节数
    }

}
