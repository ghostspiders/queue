package org.queue.javaapi;

import org.queue.message.ByteBufferMessageSet;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class MultiFetchResponse implements Iterable<ByteBufferMessageSet> {
    private final ByteBuffer underlyingBuffer; // 底层的ByteBuffer包装器
    private final short errorCode; // 错误代码
    private final org.queue.api.MultiFetchResponse underlying; // 底层的MultiFetchResponse实现

    // 构造函数
    public MultiFetchResponse(ByteBuffer buffer, int numSets, long[] offsets) {
        this.underlyingBuffer = ByteBuffer.wrap(buffer.array()); // 设置缓冲区的初始位置
        this.errorCode = buffer.getShort(); // 获取错误代码
        // 初始化底层MultiFetchResponse实现（假设存在这样的类）
        this.underlying = new org.queue.api.MultiFetchResponse(underlyingBuffer, numSets, offsets);
    }

    @Override
    public String toString() {
        return underlying.toString(); // 返回底层响应的字符串表示
    }

    @Override
    public Iterator<ByteBufferMessageSet> iterator() {
        return new Iterator<ByteBufferMessageSet>() {
            private final Iterator<ByteBufferMessageSet> iter = underlying.iterator(); // 底层迭代器

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public ByteBufferMessageSet next() {
                if (hasNext()) {
                    return iter.next();
                } else {
                    throw new NoSuchElementException("No more elements");
                }
            }
        };
    }
}