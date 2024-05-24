package org.queue.producer;

/**
 * @author gaoyvfeng
 * @ClassName Partitioner
 * @description:
 * @datetime 2024年 05月 24日 16:22
 * @version: 1.0
 */
/**
 * 一个用于数据分区的特质，它使用键来计算分区桶ID，
 * 以将数据路由到适当的代理分区。
 * @param <T> 键的类型
 */
public interface Partitioner<T> {
    /**
     * 使用键来计算分区桶ID，以便将数据路由到适当的代理分区。
     * @param key 分区的键
     * @param numPartitions 分区总数
     * @return 一个介于0和numPartitions-1之间的整数，表示分区索引
     */
    int partition(T key, int numPartitions);
}