package org.queue.message;

/**
 * @author gaoyvfeng
 * @ClassName NoCompressionCodec
 * @description:
 * @datetime 2024年 05月 23日 11:06
 * @version: 1.0
 */
/**
 * 表示没有压缩的压缩编解码器。
 */
public  class NoCompressionCodec implements CompressionCodec {
    @Override
    public int getCodec() {
        return 0;
    }
}