package org.queue.producer;

/**
 * @author gaoyvfeng
 * @ClassName DefaultPartitioner
 * @description:
 * @datetime 2024年 05月 24日 16:32
 * @version: 1.0
 */
import java.util.Random;

/**
 * 默认分区器实现，使用键的hashCode来计算分区桶ID，
 * 如果键为null，则使用随机数。
 * @param <T> 键的类型
 */
public class DefaultPartitioner<T> implements Partitioner<T> {
    private final Random random = new Random(); // 用于生成随机数

    /**
     * 根据键计算分区索引。
     * 如果键为null，则返回一个随机的分区索引。
     * 否则，使用键的hashCode对分区总数进行取模，以确定分区索引。
     * @param key 分区的键
     * @param numPartitions 分区总数
     * @return 分区索引
     */
    @Override
    public int partition(T key, int numPartitions) {
        if (key == null) {
            // 如果键为null，返回一个随机的分区索引
            return random.nextInt(numPartitions);
        } else {
            // 否则，使用键的hashCode对分区总数进行取模，以确定分区索引
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }
}