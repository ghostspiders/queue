package org.queue.utils;

/**
 * 表示一个具有起始和结束的通用范围值。
 */
public interface Range {
    /**
     * 范围内的第一个索引。
     * @return 起始索引
     */
    long start();

    /**
     * 范围内的总索引数。
     * @return 大小
     */
    long size();

    /**
     * 如果范围为空，则返回 true。
     * @return 是否为空
     */
    default boolean isEmpty() {
        return size() == 0;
    }

    /**
     * 判断给定的值是否在范围内。
     * @param value 要检查的值
     * @return 如果值在范围内，则返回 true
     */
    default boolean contains(long value) {
        if ((size() == 0 && value == start()) ||
                (size() > 0 && value >= start() && value <= start() + size() - 1)) {
            return true;
        } else {
            return false;
        }
    }
}