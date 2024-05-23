package org.queue.message;

/**
 * @author gaoyvfeng
 * @ClassName GZIPCompressionCodec
 * @description:
 * @datetime 2024年 05月 23日 11:14
 * @version: 1.0
 */
/**
 * 表示GZIP压缩的压缩编解码器。
 */
public class GZIPCompressionCodec implements CompressionCodec {
    @Override
    public int getCodec() {
        return 1;
    }
}