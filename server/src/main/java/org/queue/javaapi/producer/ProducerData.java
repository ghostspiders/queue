//package org.queue.javaapi.producer;
//
//import java.util.List;
//
///**
// * 生产者数据类，用于封装消息的topic、key和数据列表。
// * @param <K> key的类型
// * @param <V> value的类型
// */
//public class ProducerData<K, V> {
//    private final String topic;
//    private final K key;
//    private final List<V> data;
//
//    /**
//     * 构造函数，创建带有topic和数据列表的ProducerData。
//     * @param topic 消息的topic
//     * @param key 消息的key，可以为null
//     * @param data 消息的数据列表
//     */
//    public ProducerData(String topic, K key, List<V> data) {
//        this.topic = topic;
//        this.key = key;
//        this.data = data;
//    }
//
//    /**
//     * 单数据构造函数，创建带有topic和单个数据的ProducerData。
//     * @param t 消息的topic
//     * @param d 消息的数据
//     */
//    public ProducerData(String t, V d) {
//        this(t, null, asList(d));
//    }
//
//    /**
//     * 无key构造函数，创建带有topic和数据列表的ProducerData，key为null。
//     * @param t 消息的topic
//     * @param d 消息的数据列表
//     */
//    public ProducerData(String t, List<V> d) {
//        this(t, null, d);
//    }
//
//    /**
//     * 获取消息的topic。
//     * @return topic
//     */
//    public String getTopic() {
//        return topic;
//    }
//
//    /**
//     * 获取消息的key。
//     * @return key，可能为null
//     */
//    public K getKey() {
//        return key;
//    }
//
//    /**
//     * 获取消息的数据列表。
//     * @return 数据列表
//     */
//    public List<V> getData() {
//        return data;
//    }
//}