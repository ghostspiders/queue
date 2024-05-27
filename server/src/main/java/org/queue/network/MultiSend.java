package org.queue.network;

/**
 * @author gaoyvfeng
 * @ClassName MultiSend
 * @description:
 * @datetime 2024年 05月 22日 10:56
 * @version: 1.0
 */
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * 抽象类MultiSend，继承自Send，用于管理多个Send对象的发送操作。
 * @param <S> 发送对象的类型，S是Send的子类。
 */
public abstract class MultiSend<S extends Send> extends Send {
    protected int expectedBytesToWrite; // 预期要写入的总字节数
    protected List<S> current = new ArrayList<>(); // 当前要发送的Send对象列表
    private int totalWritten = 0; // 已写入的总字节数

    public MultiSend(List<S> sends) {
        this.current = sends;
    }

    @Override
    public int writeTo(WritableByteChannel channel) throws IOException {
        expectIncomplete();
        S head = current.get(0); // 获取当前列表的第一个Send对象
        int written = head.writeTo(channel); // 调用其writeTo方法写入数据
        totalWritten += written; // 更新已写入的总字节数
        if (head.complete()) {
            current.remove(0); // 如果发送完成，从列表中移除该Send对象
        }
        return written;
    }

    @Override
    public boolean complete() {
        if (current.isEmpty()) { // 如果当前列表为空
            if (totalWritten != expectedBytesToWrite) {
                // 如果实际写入的字节数与预期不符，则记录错误日志
                logger.error("Mismatch in sending bytes over socket; expected: " + expectedBytesToWrite + " actual: " + totalWritten);
            }
            return true; // 返回true，表示发送完成
        } else {
            return false; // 返回false，表示发送未完成
        }
    }
}