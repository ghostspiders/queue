package org.queue.producer;

import org.queue.cluster.Partition;

/**
 * @author gaoyvfeng
 * @ClassName ProducerPoolData
 * @description:
 * @datetime 2024年 05月 29日 10:03
 * @version: 1.0
 */
public class ProducerPoolData<V> {
    // 主题名称
    private final String topic;
    // 代理ID和分区ID
    private final Partition bidPid;
    // 要发送的数据序列
    private final Iterable<V> data;

    /**
     * 构造函数。
     * @param topic 主题名称
     * @param bidPid 代理ID和分区ID的组合对象
     * @param data 要发送的数据序列
     */
    public ProducerPoolData(String topic, Partition bidPid, Iterable<V> data) {
        this.topic = topic;
        this.bidPid = bidPid;
        this.data = data;
    }

    /**
     * 获取主题名称。
     * @return 主题名称
     */
    public String getTopic() {
        return topic;
    }

    /**
     * 获取代理ID和分区ID。
     * @return 代理ID和分区ID的组合对象
     */
    public Partition getBidPid() {
        return bidPid;
    }

    /**
     * 获取要发送的数据序列。
     * @return 数据序列
     */
    public Iterable<V> getData() {
        return data;
    }
}
