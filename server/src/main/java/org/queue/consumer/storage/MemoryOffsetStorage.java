package org.queue.consumer.storage;

/**
 * @author gaoyvfeng
 * @ClassName MemoryOffsetStorage
 * @description:
 * @datetime 2024年 05月 22日 16:07
 * @version: 1.0
 */
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 实现OffsetStorage接口的内存偏移量存储类。
 */
public class MemoryOffsetStorage implements OffsetStorage {

    // 使用线程安全的ConcurrentHashMap来存储每个节点和主题的偏移量和锁
    private ConcurrentHashMap<Integer, Lock> offsetAndLock = new ConcurrentHashMap<>();

    // 实现reserve方法，预留一个给定大小的偏移量范围
    @Override
    public long reserve(int node, String topic) {
        // 创建一个键，由节点和主题组成
        Integer key = createKey(node, topic);
        // 如果映射中不包含该键，则原子性地添加一个新的偏移量和锁
        offsetAndLock.computeIfAbsent(key, k -> new ReentrantLock());
        Lock lock = offsetAndLock.get(key);
        lock.lock(); // 获取锁
        try {
            // 假设AtomicLong对象存储在另一个ConcurrentHashMap中
            AtomicLong offset = getOrCreateOffset(node, topic);
            return offset.get();
        } finally {
            lock.unlock(); // 释放锁
        }
    }

    // 实现commit方法，更新偏移量到新的值
    @Override
    public void commit(int node, String topic, long offset) {
        // 获取高水位（偏移量）和锁
        Lock lock = offsetAndLock.get(createKey(node, topic));
        lock.lock(); // 获取锁
        try {
            AtomicLong highwater = getOrCreateOffset(node, topic);
            highwater.set(offset); // 设置新的偏移量
        } finally {
            lock.unlock(); // 释放锁
        }
    }

    // 辅助方法，用于创建键
    private Integer createKey(int node, String topic) {
        return node * 10000 + topic.hashCode(); // 示例：简单基于节点ID和主题名称的哈希码来生成键
    }

    // 辅助方法，用于获取或创建AtomicLong对象
    private AtomicLong getOrCreateOffset(int node, String topic) {
        // 假设有一个线程安全的存储来保存AtomicLong对象
        ConcurrentHashMap<String, AtomicLong> offsets = new ConcurrentHashMap<>();
        String key = "node_" + node + "_topic_" + topic;
        return offsets.computeIfAbsent(key, k -> new AtomicLong(0));
    }
}
