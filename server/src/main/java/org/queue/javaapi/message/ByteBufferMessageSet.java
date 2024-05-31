//package org.queue.javaapi.message;
//
//
//import org.queue.common.ErrorMapping;
//
//
//import org.queue.common.ErrorMapping;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import java.nio.ByteBuffer;
//import java.nio.channels.WritableByteChannel;
//import java.util.List;
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.io.IOException;
//
//public class ByteBufferMessageSet extends MessageSet {
//    private final ByteBuffer buffer;
//    private final long initialOffset;
//    private final int errorCode;
//    private static final Logger logger = LoggerFactory.getLogger(ByteBufferMessageSet.class);
//
//    // 假设org.queue.message.ByteBufferMessageSet存在，并提供必要的方法
//    private final org.queue.message.ByteBufferMessageSet underlying;
//
//    public ByteBufferMessageSet(ByteBuffer buffer, long initialOffset, int errorCode) {
//        this.buffer = buffer;
//        this.initialOffset = initialOffset;
//        this.errorCode = errorCode;
//        this.underlying = new org.queue.message.ByteBufferMessageSet(buffer, initialOffset, errorCode);
//    }
//
//    public ByteBufferMessageSet(ByteBuffer buffer) {
//        this(buffer, 0L, ErrorMapping.NoError);
//    }
//
//    public ByteBufferMessageSet(CompressionType compressionCodec, List<Message> messages) {
//        this(createByteBuffer(compressionCodec, messages), 0L, ErrorMapping.NoError);
//    }
//
//    public ByteBufferMessageSet(List<Message> messages) {
//        this(CompressionType.NONE, messages);
//    }
//
//    public long validBytes() {
//        return underlying.validBytes();
//    }
//
//    public ByteBuffer serialized() {
//        return underlying.serialized();
//    }
//
//    public long getInitialOffset() {
//        return initialOffset;
//    }
//
//    public ByteBuffer getBuffer() {
//        return buffer;
//    }
//
//    public int getErrorCode() {
//        return errorCode;
//    }
//
//    @Override
//    public Iterator<MessageAndOffset> iterator() {
//        return new Iterator<MessageAndOffset>() {
//            private final Iterator<MessageAndOffset> underlyingIterator = underlying.iterator();
//
//            @Override
//            public boolean hasNext() {
//                return underlyingIterator.hasNext();
//            }
//
//            @Override
//            public MessageAndOffset next() {
//                if (!hasNext()) {
//                    throw new NoSuchElementException();
//                }
//                return underlyingIterator.next();
//            }
//        };
//    }
//
//    @Override
//    public String toString() {
//        return underlying.toString();
//    }
//
//    public long sizeInBytes() {
//        return underlying.sizeInBytes();
//    }
//
//    @Override
//    public boolean equals(Object other) {
//        if(other instanceof ByteBufferMessageSet){
//            ByteBufferMessageSet that = (ByteBufferMessageSet) other;
//            return errorCode == that.errorCode &&
//                    initialOffset == that.initialOffset &&
//                    buffer.equals(that.buffer);
//        }else {
//            return false;
//        }
//    }
//
//    @Override
//    public int hashCode() {
//        int result = errorCode;
//        result = 31 * result + buffer.hashCode();
//        result = 31 * result + (int) (initialOffset ^ (initialOffset >>> 32));
//        return result;
//    }
//
//    @Override
//    public long writeTo(WritableByteChannel channel, long offset, long maxSize) throws IOException {
//        // 根据提供的offset和maxSize将消息写入WritableByteChannel
//        // 这里需要实现具体的写入逻辑，下面代码是示例性的
//        long bytesWritten = 0;
//        while (bytesWritten < maxSize && buffer.hasRemaining()) {
//            channel.write(buffer.slice());
//            bytesWritten += buffer.position();
//            buffer.position(0); // Reset position after write
//        }
//        return bytesWritten;
//    }
//
//    private ByteBuffer createByteBuffer(CompressionType compressionCodec, List<Message> messages) {
//        // 根据压缩类型和消息列表创建ByteBuffer
//        // 这里需要实现具体的序列化和压缩逻辑，下面代码是示例性的
//        ByteBuffer buffer = ByteBuffer.allocate(1024); // 假设分配了足够的空间
//        for (Message message : messages) {
//            message.serializeTo(buffer);
//        }
//        buffer.flip();
//        return buffer;
//    }
//}