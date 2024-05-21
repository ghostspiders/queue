package org.queue.api;

import org.queue.common.ErrorMapping;
import org.queue.network.Request;
import org.queue.network.RequestKeys;
import org.queue.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class OffsetRequest extends Request {
    public static final String SmallestTimeString = "smallest";
    public static final String LargestTimeString = "largest";
    public static final long LatestTime = -1L;
    public static final long EarliestTime = -2L;

    private String topic;
    private int partition;
    private long time;
    private int maxNumOffsets;

    public OffsetRequest(String topic, int partition, long time, int maxNumOffsets) {
        super(RequestKeys.Offsets);
        this.topic = topic;
        this.partition = partition;
        this.time = time;
        this.maxNumOffsets = maxNumOffsets;
    }

    public static OffsetRequest readFrom(ByteBuffer buffer) {
        String topic = Utils.readShortString(buffer, StandardCharsets.UTF_8);
        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxNumOffsets = buffer.getInt();
        return new OffsetRequest(topic, partition, offset, maxNumOffsets);
    }

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

    public static long[] deserializeOffsetArray(ByteBuffer buffer) {
        int size = buffer.getInt();
        long[] offsets = new long[size];
        for (int i = 0; i < size; i++) {
            offsets[i] = buffer.getLong();
        }
        return offsets;
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        Utils.writeShortString(buffer, topic, StandardCharsets.UTF_8);
        buffer.putInt(partition);
        buffer.putLong(time);
        buffer.putInt(maxNumOffsets);
    }

    public int sizeInBytes() {
        return 2 + topic.length() + 4 + 8 + 4;
    }

    @Override
    public String toString() {
        return "OffsetRequest(topic:" + topic + ", part:" + partition + ", time:" + time +
                ", maxNumOffsets:" + maxNumOffsets + ")";
    }
}

// 假设有一个Send类可以被扩展
class OffsetArraySend extends Send {
    private long[] offsets;
    private long size;
    private ByteBuffer header;
    private ByteBuffer contentBuffer;
    private boolean complete;

    public OffsetArraySend(long[] offsets) {
        super(); // 假设构造函数需要调用父类构造函数
        this.offsets = offsets;
        this.size = offsets.length > 0 ? 4 + 8 * offsets.length : 0;
        this.header = ByteBuffer.allocate(6);
        this.header.putInt((int)(size + 2));
        this.header.putShort(ErrorMapping.NoError);
        this.header.rewind();
        this.contentBuffer = serializeOffsetArray(offsets);
        this.complete = false;
    }

    private ByteBuffer serializeOffsetArray(long[] offsets) {
        ByteBuffer buffer = ByteBuffer.allocate(8 * offsets.length);
        for (long offset : offsets) {
            buffer.putLong(offset);
        }
        buffer.rewind();
        return buffer;
    }

    public int writeTo(WritableByteChannel channel) throws IOException {
        checkExpectIncomplete();
        int written = 0;
        if (header.hasRemaining()) {
            written += channel.write(header);
        }
        if (!header.hasRemaining() && contentBuffer.hasRemaining()) {
            written += channel.write(contentBuffer);
        }

        if (!contentBuffer.hasRemaining()) {
            complete = true;
        }
        return written;
    }

    private void checkExpectIncomplete() {
        if (complete) {
            throw new IllegalStateException("Write operation is already complete.");
        }
    }
}
