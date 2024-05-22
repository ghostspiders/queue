package org.queue.log1;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 一个写时复制列表实现，提供一致的视图。
 * view()方法提供了一个不可变的序列，代表列表的一致状态。
 * 用户可以在不锁定对列表的所有访问的情况下，对这个序列执行迭代操作，例如二分查找。
 * 即使底层列表的范围发生变化，视图也不会有任何变化。
 */
class SegmentList<T> {
    // 写时复制的内容，使用AtomicReference保证线程安全
    private AtomicReference<T[]> contents;

    public SegmentList(java.util.List<T> seq) {
        // 使用序列创建SegmentList，并转换成数组
        this.contents = new AtomicReference<>(seq.toArray((T[])new Object[0]));
    }

    /**
     * 将给定项追加到列表末尾
     */
    public void append(T... ts) {
        while (true) {
            T[] curr = contents.get();
            T[] updated = Arrays.copyOf(curr, curr.length + ts.length);
            System.arraycopy(ts, 0, updated, curr.length, ts.length);
            if (contents.compareAndSet(curr, updated)) {
                return;
            }
        }
    }

    /**
     * 从列表中删除前n项
     */
    public T[] trunc(int newStart) {
        if (newStart < 0) {
            throw new IllegalArgumentException("起始索引必须是正数。");
        }
        T[] deleted = null;
        T[] curr;
        T[] updated;
        boolean done = false;
        while (!done) {
            curr = contents.get();
            int newLength = Math.max(curr.length - newStart, 0);
            updated = Arrays.copyOfRange(curr, newStart, curr.length);
            if (contents.compareAndSet(curr, updated)) {
                deleted = Arrays.copyOfRange(curr, 0, curr.length - newLength);
                done = true;
            }
        }
        return deleted;
    }

    /**
     * 获取序列的一致视图
     */
    public T[] view() {
        return contents.get();
    }

    /**
     * 更友好的toString方法
     */
    @Override
    public String toString() {
        return Arrays.toString(view());
    }
}

/**
 * SegmentList类使用的常量
 */
class SegmentListConstants {
    // 最大尝试次数
    public static final int MaxAttempts = 20;
}
