package org.queue.message;

/**
 * @author gaoyvfeng
 * @ClassName Message
 * @description:
 * @datetime 2024年 05月 23日 15:03
 * @version: 1.0
 */
import java.nio.ByteBuffer;
import java.util.Arrays;

// 假设CompressionCodec接口和NoCompressionCodec类已经定义好了
// import your.package.CompressionCodec;
// import your.package.NoCompressionCodec;

public class Message {
    // 消息的头部大小常量，需要根据实际情况定义
    private static final int MagicOffset = 0;
    private static final int AttributeOffset = 1;
    private static final int CurrentMagicValue = 1; // 假设当前魔数值为1
    private static final int CompressionCodeMask = 0xFF; // 压缩代码掩码

    // ByteBuffer用于存储整个消息的数据
    private ByteBuffer buffer;

    // 私有构造函数，用于创建消息对象
    private Message(long checksum, byte[] bytes, CompressionCodec compressionCodec) {
        this(ByteBuffer.allocate(headerSize(CurrentMagicValue) + bytes.length));
        buffer.put((byte)CurrentMagicValue); // 写入魔数
        byte attributes = 0;
        if (compressionCodec.getCodec() > 0) {
            attributes = (byte)((attributes | (CompressionCodeMask & compressionCodec.getCodec())) & 0xFF);
        }
        buffer.put(attributes); // 写入属性
        Utils.putUnsignedInt(buffer, checksum); // 写入校验和
        buffer.put(bytes); // 写入字节数据
        buffer.rewind(); // 重置buffer的位置为0
    }

    // 构造函数重载，用于不指定压缩编解码器时使用默认NoCompressionCodec
    public Message(long checksum, byte[] bytes) {
        this(checksum, bytes, NoCompressionCodec.INSTANCE);
    }

    // 构造函数重载，用于指定压缩编解码器
    public Message(byte[] bytes, CompressionCodec compressionCodec) {
        this(Utils.crc32(bytes), bytes, compressionCodec);
    }

    // 构造函数重载，用于不指定压缩编解码器时使用默认NoCompressionCodec
    public Message(byte[] bytes) {
        this(bytes, NoCompressionCodec.INSTANCE);
    }

    // 获取整个消息的大小
    public int size() {
        return buffer.limit();
    }

    // 获取负载的大小，不包括头部
    public int payloadSize() {
        return size() - headerSize(magic());
    }

    // 获取魔数
    public byte magic() {
        return buffer.get(MagicOffset);
    }

    // 获取属性
    public byte attributes() {
        return buffer.get(AttributeOffset);
    }

    // 根据属性获取压缩编解码器
    public CompressionCodec compressionCodec() {
        switch (magic()) {
            case 0:
                return NoCompressionCodec.INSTANCE;
            case 1:
                return CompressionCodec.getCompressionCodec(attributes() & CompressionCodeMask);
            default:
                throw new RuntimeException("Invalid magic byte " + magic());
        }
    }

    // 获取校验和
    public long checksum() {
        return Utils.getUnsignedInt(buffer, crcOffset(magic()));
    }

    // 获取负载的ByteBuffer
    public ByteBuffer payload() {
        ByteBuffer payload = buffer.duplicate();
        payload.position(headerSize(magic()));
        payload = payload.slice();
        payload.limit(payloadSize());
        payload.rewind();
        return payload;
    }

    // 验证消息是否有效
    public boolean isValid() {
        return checksum() == Utils.crc32(buffer.array(), buffer.position() + buffer.arrayOffset() + payloadOffset(magic()), payloadSize());
    }

    // 计算序列化后的大小
    public int serializedSize() {
        return 4 /* int大小 */ + buffer.limit();
    }

    // 序列化到ByteBuffer
    public void serializeTo(ByteBuffer serBuffer) {
        serBuffer.putInt(buffer.limit());
        serBuffer.put(buffer.duplicate());
    }

    // 重写toString方法，方便打印信息
    @Override
    public String toString() {
        return String.format("message(magic = %d, attributes = %d, crc = %d, payload = %s)", magic(), attributes(), checksum(), Arrays.toString(payload().array()));
    }

    // 重写equals方法，比较两个Message对象是否相等
    @Override
    public boolean equals(Object any) {
        if (any instanceof Message) {
            Message that = (Message) any;
            return size() == that.size() && attributes() == that.attributes() && checksum() == that.checksum() &&
                    Arrays.equals(payload().array(), that.payload().array()) && magic() == that.magic();
        }
        return false;
    }

    // 重写hashCode方法
    @Override
    public int hashCode() {
        return buffer.hashCode();
    }

    // 辅助方法，需要根据实际情况进行定义
    private int headerSize(int magic) {
        // 实现获取头部大小的逻辑
        return 0; // 这里只是一个示例，需要根据实际情况实现
    }

    private int crcOffset(byte magic) {
        // 实现获取crc校验和偏移量的逻辑
        return 0; // 这里只是一个示例，需要根据实际情况实现
    }

    private int payloadOffset(byte magic) {
        // 实现获取负载偏移量的逻辑
        return 0; // 这里只是一个示例，需要根据实际情况实现
    }
}