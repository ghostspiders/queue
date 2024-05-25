package org.queue.utils;

/**
 * 一个可模拟的时间功能接口
 */
public interface Time {

    /**
     * 返回当前时间的毫秒表示。
     * @return 当前时间的毫秒数
     */
    long milliseconds();

    /**
     * 返回当前时间的纳秒表示。
     * @return 当前时间的纳秒数
     */
    long nanoseconds();

    /**
     * 使当前线程暂停指定的毫秒数。
     * @param ms 要暂停的毫秒数
     */
    void sleep(long ms);
}