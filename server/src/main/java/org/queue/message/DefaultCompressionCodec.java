package org.queue.message;

/**
 * @author gaoyvfeng
 * @ClassName DefaultCompressionCodec
 * @description:
 * @datetime 2024年 05月 23日 11:14
 * @version: 1.0
 */
/**
 * 表示默认压缩的压缩编解码器。
 */
public class DefaultCompressionCodec implements CompressionCodec {
    @Override
    public int getCodec() {
        return 1;
    }
}