package org.queue.utils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 一个泛型池类，实现了Iterable接口，可以存储键值对。
 */
public class Pool<K, V> implements Iterable<Map.Entry<K, V>> {

    private ConcurrentHashMap<K, V> pool;

    public Pool() {
        this.pool = new ConcurrentHashMap<>();
    }

    /**
     * 使用给定的Map构造Pool对象，并初始化池内容。
     * @param m 初始化Map
     */
    public Pool(Map<K, V> m) {
        this();
        for (Map.Entry<K, V> entry : m.entrySet()) {
            pool.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 将指定的值与此池中的指定键关联（可选操作）。
     * 如果键不存在，则返回null，否则返回键的旧值。
     * @param k 键
     * @param v 值
     * @return 如果键之前存在，则返回旧值，否则返回null
     */
    public V put(K k, V v) {
        return pool.put(k, v);
    }

    /**
     * 如果此池中没有键的映射存在，则将键和值放入池中。
     * @param k 键
     * @param v 值
     * @return 如果之前键不存在，则返回null，否则返回已经存在的值
     */
    public V putIfNotExists(K k, V v) {
        return pool.putIfAbsent(k, v);
    }

    /**
     * 如果此池包含指定的键，则返回true。
     * @param id 键
     * @return 如果包含指定键，则返回true
     */
    public boolean contains(K id) {
        return pool.containsKey(id);
    }

    /**
     * 返回此池中指定键的值。
     * @param key 键
     * @return 与指定键关联的值
     */
    public V get(K key) {
        return pool.get(key);
    }

    /**
     * 如果存在一个键的映射，则将其从池中移除。
     * @param key 要移除的键
     * @return 如果存在一个键的映射，则返回其值
     */
    public V remove(K key) {
        return pool.remove(key);
    }

    /**
     * 返回池中包含的键的Set视图。
     * @return 键的Set
     */
    public Set<K> keys() {
        return pool.keySet();
    }

    /**
     * 返回一个包含池中所有值的Iterable。
     * @return 包含所有值的Iterable
     */
    public Iterable<V> values() {
        return new ArrayList<>(pool.values());
    }

    /**
     * 从池中移除所有元素。
     */
    public void clear() {
        pool.clear();
    }

    /**
     * 返回池中元素的数量。
     * @return 元素数量
     */
    public int size() {
        return pool.size();
    }

    /**
     * 返回一个遍历此池的迭代器。
     * @return 迭代器
     */
    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        return new Iterator<Map.Entry<K, V>>() {
            private final Iterator<Map.Entry<K, V>> iter = pool.entrySet().iterator();

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public Map.Entry<K, V> next() {
                return iter.next();
            }
        };
    }
}