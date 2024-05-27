package org.queue.utils;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DelayedItem<T> implements Delayed {
    private final T item;
    private final long delayMs; // 延迟时间，以毫秒为单位
    private final long createdMs; // 对象创建的时间，用于计算延迟

    // 构造函数，接受延迟时间的单位
    public DelayedItem(T item, long delay, TimeUnit unit) {
        this.item = item;
        this.delayMs = unit.toMillis(delay);
        this.createdMs = System.currentTimeMillis();
    }

    // 重载构造函数，使用毫秒作为延迟时间的单位
    public DelayedItem(T item, long delayMs) {
        this(item, delayMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 获取剩余的延迟时间
     * @param unit 时间单位
     * @return 返回剩余的延迟时间
     */
    @Override
    public long getDelay(TimeUnit unit) {
        long elapsedMs = (System.currentTimeMillis() - createdMs);
        long timeLeftMs = Math.max(delayMs - elapsedMs, 0);
        return unit.convert(timeLeftMs, TimeUnit.MILLISECONDS);
    }

    // 实现Delayed接口的compareTo方法，用于在优先队列中排序
    @Override
    public int compareTo(Delayed d) {
        if (d instanceof DelayedItem) {
            DelayedItem<?> delayed = (DelayedItem<?>) d;
            long myEnd = createdMs + delayMs;
            long yourEnd = delayed.createdMs - delayed.delayMs;

            if (myEnd < yourEnd) return -1;
            else if (myEnd > yourEnd) return 1;
            else return 0;
        } else {
            long myDelay = getDelay(TimeUnit.NANOSECONDS);
            long yourDelay = d.getDelay(TimeUnit.NANOSECONDS);
            return Long.compare(myDelay, yourDelay);
        }
    }

    // 泛型类型参数T的getter方法
    public T getItem() {
        return item;
    }
}