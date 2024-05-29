package org.queue.api;
import org.queue.utils.Utils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
public class OffsetRequest {

    public static final String SmallestTimeString = "smallest";
    public static final String LargestTimeString = "largest";
    public static final long LatestTime = -1L;
    public static final long EarliestTime = -2L;

    private String topic;
    private int partition;
    private long time;
    private int maxNumOffsets;

    // OffsetRequest构造函数
    public OffsetRequest(String topic, int partition, long time, int maxNumOffsets) {
        this.topic = topic;
        this.partition = partition;
        this.time = time;
        this.maxNumOffsets = maxNumOffsets;
    }

    // 从ByteBuffer中读取数据并创建OffsetRequest对象
    public static OffsetRequest readFrom(ByteBuffer buffer) {
        String topic = Utils.readShortString(buffer, StandardCharsets.UTF_8);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxNumOffsets = buffer.getInt();
        return new OffsetRequest(topic, partition, offset, maxNumOffsets);
    }

    // 序列化长整型数组为ByteBuffer
    public static ByteBuffer serializeOffsetArray(long[] offsets) {
        int size = 4 + 8 * offsets.length;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putInt(offsets.length);
        for (long offset : offsets) {
            buffer.putLong(offset);
        }
        buffer.rewind();
        return buffer;
    }

    // 反序列化ByteBuffer为长整型数组
    public static long[] deserializeOffsetArray(ByteBuffer buffer) {
        int size = buffer.getInt();
        long[] offsets = new long[size];
        for (int i = 0; i < size; i++) {
            offsets[i] = buffer.getLong();
        }
        return offsets;
    }

    // 将OffsetRequest对象的数据写入ByteBuffer中
    public void writeTo(ByteBuffer buffer) {
        Utils.writeShortString(buffer, topic, StandardCharsets.UTF_8);
        buffer.putInt(partition);
        buffer.putLong(time);
        buffer.putInt(maxNumOffsets);
    }

    // 计算OffsetRequest对象的字节大小
    public int sizeInBytes() {
        return 2 + topic.length() + 4 + 8 + 4;
    }

    // 返回OffsetRequest对象的字符串表示
    @Override
    public String toString() {
        return "OffsetRequest(topic:" + topic + ", part:" + partition + ", time:" + time +
                ", maxNumOffsets:" + maxNumOffsets + ")";
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getTime() {
        return time;
    }

    public int getMaxNumOffsets() {
        return maxNumOffsets;
    }
}
