package org.queue.api;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.queue.network.Request;
import org.queue.utils.Utils;

public class FetchRequest extends Request {
    private String topic;
    private int partition;
    private long offset;
    private int maxSize;

    // 私有构造函数，由伴生对象的方法调用
    public FetchRequest(String topic, int partition, long offset, int maxSize) {
        super(RequestKeys.fetch); // 调用父类的构造函数
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.maxSize = maxSize;
    }

    public FetchRequest() {
        super(RequestKeys.fetch);
    }

    //从ByteBuffer中读取数据创建FetchRequest对象
    public static FetchRequest readFrom(ByteBuffer buffer) {
        String topic = Utils.readShortString(buffer, StandardCharsets.UTF_8);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int size = buffer.getInt();
        return new FetchRequest(topic, partition, offset, size);
    }

    // 将FetchRequest对象的数据写入ByteBuffer
    public void writeTo(ByteBuffer buffer) {
        Utils.writeShortString(buffer, topic, StandardCharsets.UTF_8);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxSize);
    }

    // 计算请求的字节大小
    public int sizeInBytes() {
        // 计算字符串长度占用的字节数，加上字符串本身的字节数，加上其他整型和长整型占用的字节数
        return 2 + topic.length() + 4 + 8 + 4;
    }

    // 重写toString方法
    @Override
    public String toString() {
        return "FetchRequest(topic:" + topic + ", part:" + partition + ", offset:" + offset +
                ", maxSize:" + maxSize + ")";
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public int getMaxSize() {
        return maxSize;
    }
}
