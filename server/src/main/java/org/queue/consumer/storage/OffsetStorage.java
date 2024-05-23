package org.queue.consumer.storage;

/**
 * @author gaoyvfeng
 * @ClassName OffsetStorage
 * @description:
 * @datetime 2024年 05月 22日 16:03
 * @version: 1.0
 */
/**
 * 用于存储消费者偏移量的接口。
 * 该接口用于跟踪消费者在数据流中的进度。
 */
public interface OffsetStorage {

    /**
     * 预留一个给定大小的偏移量范围。
     * @param node 节点标识符
     * @param topic 主题名称
     * @return 预留的范围
     */
    long reserve(int node, String topic);

    /**
     * 更新偏移量到新的值。
     * @param node 节点标识符
     * @param topic 主题名称
     * @param offset 新的偏移量
     */
    void commit(int node, String topic, long offset);
}
