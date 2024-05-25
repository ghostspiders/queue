package org.queue.utils;

import java.util.concurrent.TimeUnit;

/**
 * 一个模拟时间的类，用于测试或其他需要控制时间的场景。
 */
public class MockTime implements Time {
    private volatile long currentMs;

    /**
     * 使用当前系统时间毫秒数构造MockTime对象。
     */
    public MockTime() {
        this(System.currentTimeMillis());
    }

    /**
     * 使用给定的时间毫秒数构造MockTime对象。
     * @param currentMs 时间毫秒数
     */
    public MockTime(long currentMs) {
        this.currentMs = currentMs;
    }

    /**
     * 获取当前模拟的时间，单位为毫秒。
     * @return 当前模拟的时间毫秒数
     */
    public long milliseconds() {
        return currentMs;
    }

    /**
     * 获取当前模拟的时间，单位为纳秒。
     * @return 当前模拟的时间纳秒数
     */
    public long nanoseconds() {
        return TimeUnit.NANOSECONDS.convert(currentMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 模拟时间的流逝。
     * @param ms 要增加的毫秒数
     */
    public void sleep(long ms) {
        currentMs += ms;
    }
}