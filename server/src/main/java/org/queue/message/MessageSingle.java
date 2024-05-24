package org.queue.message;

/**
 * @author gaoyvfeng
 * @ClassName MessageSingle
 * @description:
 * @datetime 2024年 05月 23日 15:15
 * @version: 1.0
 */
import org.queue.common.UnknownMagicByteException;
import org.queue.utils.Utils;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * 消息字节偏移量
 */
public class MessageSingle {

    // 定义消息魔术字节的版本和相关常量
    public static final byte MAGIC_VERSION_1 = 0;
    public static final byte MAGIC_VERSION_2 = 1;
    public static final byte CURRENT_MAGIC_VALUE = 1;
    private static final int MAGIC_OFFSET = 0;
    private static final int MAGIC_LENGTH = 1;
    private static final int ATTRIBUTE_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
    private static final int ATTRIBUTE_LENGTH = 1;
    private static final int COMPRESSION_CODE_MASK = 0x03;  // 用于压缩代码的掩码，2位用于表示压缩编解码器
    public static final int NO_COMPRESSION = 0;

    // CRC长度
    private static final int CRC_LENGTH = 4;

    /**
     * 根据魔术字节计算CRC值偏移量
     *
     * @param magic 魔术字节
     * @return CRC值偏移量
     */
    public static int crcOffset(byte magic) {
        switch (magic) {
            case MAGIC_VERSION_1:
                return MAGIC_OFFSET + MAGIC_LENGTH;
            case MAGIC_VERSION_2:
                return ATTRIBUTE_OFFSET + ATTRIBUTE_LENGTH;
            default:
                throw new UnknownMagicByteException("Magic byte value of " + magic + " is unknown");
        }
    }

    /**
     * 根据魔术字节计算消息负载偏移量
     *
     * @param magic 魔术字节
     * @return 负载偏移量
     */
    public static int payloadOffset(byte magic) {
        return crcOffset(magic) + CRC_LENGTH;
    }

    /**
     * 根据魔术字节计算消息头大小
     *
     * @param magic 魔术字节
     * @return 消息头大小
     */
    public static int headerSize(byte magic) {
        return payloadOffset(magic);
    }

    // 魔术字节0的消息头最小大小
    public static final int MIN_HEADER_SIZE = headerSize(MAGIC_VERSION_1);

}