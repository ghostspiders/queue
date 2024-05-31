//package org.queue.javaapi.producer;
//
//import org.queue.api.RequestKeys;
//import org.queue.network.Request;
//import java.nio.ByteBuffer;
//import java.util.Objects;
//import org.queue.javaapi.message.ByteBufferMessageSet;
//
//public class ProducerRequest extends Request {
//    // 主题名称
//    private final String topic;
//    // 分区编号
//    private final int partition;
//    // 消息集合
//    private final ByteBufferMessageSet messages;
//    // 封装的ProducerRequest对象
//    private final org.queue.api.ProducerRequest underlying;
//
//    // 构造函数
//    public ProducerRequest(String topic, int partition, ByteBufferMessageSet messages) {
//        super(RequestKeys.produce); // 调用父类构造函数，设置请求类型为生产请求
//        this.topic = topic; // 初始化主题
//        this.partition = partition; // 初始化分区
//        this.messages = messages; // 初始化消息集合
//        this.underlying = new org.queue.api.ProducerRequest(topic, partition, messages); // 初始化封装的对象
//    }
//
//    // 将请求写入ByteBuffer的方法
//    @Override
//    public void writeTo(ByteBuffer buffer) {
//        underlying.writeTo(buffer); // 委托给封装对象执行
//    }
//
//    // 获取请求的字节大小
//    @Override
//    public int sizeInBytes() {
//        return underlying.sizeInBytes(); // 委托给封装对象执行
//    }
//
//    // 获取分区的转换方法
//    public int getTranslatedPartition(java.util.function.IntUnaryOperator randomSelector) {
//        // 这里假设存在一种方法将IntUnaryOperator转换为String => Int函数接口
//        // 需要根据项目的实际需求提供这种转换
//        // 例如，使用lambda表达式：
//        // (String s) -> randomSelector.applyAsInt(Integer.parseInt(s))
//        return underlying.getTranslatedPartition(s -> randomSelector.applyAsInt(Integer.parseInt(s)));
//    }
//
//    // 返回对象的字符串表示
//    @Override
//    public String toString() {
//        return underlying.toString(); // 委托给封装对象执行
//    }
//
//    // 重写equals方法，提供基于值的相等性比较
//    @Override
//    public boolean equals(Object other) {
//        if (this == other) return true;
//        if (other == null || getClass() != other.getClass()) return false;
//        ProducerRequest that = (ProducerRequest) other;
//        return partition == that.partition &&
//                Objects.equals(topic, that.topic) &&
//                Objects.equals(messages, that.messages);
//    }
//
//    // 重写hashCode方法，与equals方法保持一致
//    @Override
//    public int hashCode() {
//        return 31 + (17 * partition) + (topic != null ? topic.hashCode() : 0) + (messages != null ? messages.hashCode() : 0);
//    }
//}
