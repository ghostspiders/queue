package org.queue.utils;

/**
 * 提供系统时间相关功能的类，实现了Time接口。
 */
public class SystemTime implements Time {

    // 1纳秒等于1000皮秒
    public static final int NsPerUs = 1000;
    // 1毫秒等于1000微秒
    public static final int UsPerMs = 1000;
    // 1秒等于1000毫秒
    public static final int MsPerSec = 1000;
    // 1毫秒等于多少纳秒
    public static final long NsPerMs = NsPerUs * UsPerMs;
    // 1秒等于多少纳秒
    public static final long NsPerSec = NsPerMs * MsPerSec;
    // 1秒等于多少微秒
    public static final long UsPerSec = UsPerMs * MsPerSec;
    // 1分钟等于多少秒
    public static final int SecsPerMin = 60;
    // 1小时等于多少分钟
    public static final int MinsPerHour = 60;
    // 1天等于多少小时
    public static final int HoursPerDay = 24;
    // 1小时等于多少秒
    public static final long SecsPerHour = SecsPerMin * MinsPerHour;
    // 1天等于多少秒
    public static final long SecsPerDay = SecsPerHour * HoursPerDay;
    // 1天等于多少分钟
    public static final long MinsPerDay = MinsPerHour * HoursPerDay;
    // 单例实例
    private static final SystemTime INSTANCE = new SystemTime();

    // 私有构造函数，防止外部创建实例
    private SystemTime() {
    }

    /**
     * 获取SystemTime的单例实例。
     * @return SystemTime单例对象
     */
    public static SystemTime getInstance() {
        return INSTANCE;
    }

    @Override
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    @Override
    public long nanoseconds() {
        return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 保持中断状态
        }
    }
}