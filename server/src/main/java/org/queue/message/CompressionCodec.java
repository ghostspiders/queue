package org.queue.message;

/**
 * @author gaoyvfeng
 * @ClassName CompressionCodec
 * @description:
 * @datetime 2024年 05月 23日 11:01
 * @version: 1.0
 */

import org.queue.common.UnknownCodecException;

/**
 * 压缩编解码器接口。
 */
public interface CompressionCodec {
    /**
     * 获取压缩编解码器的代码。
     *
     * @return 压缩编解码器的代码。
     */
    int getCodec();

    /**
     * 根据给定的整数代码获取对应的压缩编解码器。
     *
     * @param codec 压缩编解码器的代码。
     * @return 对应的压缩编解码器实例。
     * @throws UnknownCodecException 如果给定的代码不是已知的压缩编解码器。
     */
    public static CompressionCodec getCompressionCodec(int codec) throws UnknownCodecException {
        switch (codec) {
            case 0:
                return new NoCompressionCodec();
            case 1:
                return new GZIPCompressionCodec();
            default:
                throw new UnknownCodecException(String.format("%d is an unknown compression codec", codec));
        }
    }
}