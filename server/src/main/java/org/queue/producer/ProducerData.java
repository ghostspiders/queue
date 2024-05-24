package org.queue.producer;

/**
 * @author gaoyvfeng
 * @ClassName ProducerData
 * @description:
 * @datetime 2024年 05月 24日 16:34
 * @version: 1.0
 */
import java.util.List;

/**
 * 表示生产者数据的类，包含主题、键和数据序列。
 * @param <K> 键的类型
 * @param <V> 数据项的类型
 */
public class ProducerData<K, V> {
    // 主题
    private final String topic;
    // 键，可能为null
    private final K key;
    // 数据序列
    private final List<V> data;

    /**
     * 构造函数，初始化主题、键和数据。
     * @param topic 主题名称
     * @param key 分区键
     * @param data 数据序列
     */
    public ProducerData(String topic, K key, List<V> data) {
        this.topic = topic;
        this.key = key;
        this.data = data;
    }

    /**
     * 重载的构造函数，当没有键时使用，将键设置为null。
     * @param topic 主题名称
     * @param data 数据序列
     */
    public ProducerData(String topic, List<V> data) {
        this(topic, null, data);
    }

    /**
     * 重载的构造函数，当数据只有一个元素时使用，将该单个元素包装为列表。
     * @param topic 主题名称
     * @param data 单个数据元素
     */
    public ProducerData(String topic, V data) {
        this(topic, (K)null, List.of(data)); // 使用List.of来创建包含单个元素的列表
    }

    /**
     * 获取主题名称。
     * @return 主题名称
     */
    public String getTopic() {
        return topic;
    }

    /**
     * 获取分区键。
     * @return 分区键，可能为null
     */
    public K getKey() {
        return key;
    }

    /**
     * 获取数据序列。
     * @return 数据序列
     */
    public List<V> getData() {
        return data;
    }
}