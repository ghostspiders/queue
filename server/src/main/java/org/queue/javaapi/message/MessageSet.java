//package org.queue.javaapi.message;
//
//import java.util.Iterator;
//import org.queue.message.InvalidMessageException;
//import org.queue.message.MessageAndOffset;
//
//
///**
// * 消息集合。消息集合具有固定的序列化形式，尽管字节的容器可以是内存中的或磁盘上的。
// * 每个消息的格式如下：
// * 4字节大小，包含一个整数N
// * N个字节的消息，如消息类中描述的
// */
//public abstract class MessageSet implements Iterable<MessageAndOffset> {
//
//    /**
//     * 提供此集合中消息的迭代器。
//     * 实现类需要提供具体实现。
//     *
//     * @return 消息迭代器
//     */
//    @Override
//    public abstract Iterator<MessageAndOffset> iterator();
//
//    /**
//     * 给出此消息集合的总字节大小。
//     * 实现类需要提供具体实现。
//     *
//     * @return 消息集合的总字节大小
//     */
//    public abstract long sizeInBytes();
//
//    /**
//     * 验证集合中所有消息的校验和。如果任何消息的校验和与有效负载不匹配，则抛出InvalidMessageException。
//     */
//    public void validate() {
//        Iterator<MessageAndOffset> iterator = iterator();
//        while (iterator.hasNext()) {
//            MessageAndOffset messageAndOffset = iterator.next();
//            if (!messageAndOffset.message.isValid()) {
//                throw new InvalidMessageException("Invalid message checksum");
//            }
//        }
//    }
//}