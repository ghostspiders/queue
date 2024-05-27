package org.queue.message;

/**
 * @author gaoyvfeng
 * @ClassName MessageAndOffset
 * @description:
 * @datetime 2024年 05月 22日 11:01
 * @version: 1.0
 */
public class MessageAndOffset {
    private  Message message; // 消息对象
    private  long offset;    // 偏移量

    /**
     * 构造函数。
     *
     * @param message 消息对象。
     * @param offset  消息的偏移量。
     */
    public MessageAndOffset(Message message, long offset) {
        this.message = message;
        this.offset = offset;
    }

    // message字段的getter方法
    public Message getMessage() {
        return message;
    }

    // offset字段的getter方法
    public long getOffset() {
        return offset;
    }

    // 重写toString方法，提供类的字符串表示
    @Override
    public String toString() {
        return "MessageAndOffset{" +
                "message=" + message +
                ", offset=" + offset +
                '}';
    }
}