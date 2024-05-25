package org.queue.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * 用于测量和限制某个过程速率的类。
 * Throttler类接受期望的每秒速率（单位不重要，可以是字节或任何其他事物的计数），
 * 并在调用maybeThrottle()时适当地休眠以达到期望速率。
 */
public class Throttler {
    private final static Logger logger = Logger.getLogger(Throttler.class);
    private final static long DefaultCheckIntervalMs = 100L;
    private final double desiredRatePerSec;
    private final long checkIntervalMs;
    private final boolean throttleDown;
    private final Time time;
    private final Lock lock;
    private long periodStartNs;
    private double observedSoFar;

    /**
     * 使用默认检查间隔和SystemTime构造Throttler。
     * @param desiredRatePerSec 期望的每秒速率
     * @param throttleDown 是否降低速率
     */
    public Throttler(double desiredRatePerSec, boolean throttleDown) {
        this(desiredRatePerSec, DefaultCheckIntervalMs, throttleDown, SystemTime.INSTANCE);
    }

    /**
     * 使用默认检查间隔和默认SystemTime构造Throttler，且默认降低速率。
     * @param desiredRatePerSec 期望的每秒速率
     */
    public Throttler(double desiredRatePerSec) {
        this(desiredRatePerSec, DefaultCheckIntervalMs, true, SystemTime.INSTANCE);
    }

    /**
     * 构造Throttler。
     * @param desiredRatePerSec 期望的每秒速率
     * @param checkIntervalMs 检查速率的间隔（毫秒）
     * @param throttleDown 是否降低速率
     * @param time 时间实现
     */
    public Throttler(double desiredRatePerSec, long checkIntervalMs, boolean throttleDown, Time time) {
        this.desiredRatePerSec = desiredRatePerSec;
        this.checkIntervalMs = checkIntervalMs;
        this.throttleDown = throttleDown;
        this.time = time;
        this.lock = new ReentrantLock();
        this.periodStartNs = time.nanoseconds();
        this.observedSoFar = 0.0;
    }

    /**
     * 可能需要时进行节流。
     * @param observed 观察到的数量
     */
    public void maybeThrottle(double observed) {
        lock.lock();
        try {
            observedSoFar += observed;
            long now = time.nanoseconds();
            long ellapsedNs = now - periodStartNs;
            // 如果我们完成了一个间隔并且我们观察到了一些内容，也许我们应该小睡片刻
            if (ellapsedNs > checkIntervalMs * Time.NsPerMs && observedSoFar > 0) {
                double rateInSecs = (observedSoFar * Time.NsPerSec) / ellapsedNs;
                boolean needAdjustment = !(throttleDown ^ (rateInSecs > desiredRatePerSec));
                if (needAdjustment) {
                    // 解出我们需要休眠多长时间以达到期望速率
                    double desiredRateMs = desiredRatePerSec / Time.MsPerSec;
                    long ellapsedMs = ellapsedNs / Time.NsPerMs;
                    long sleepTime = Math.round(observedSoFar / desiredRateMs - ellapsedMs);
                    if (sleepTime > 0) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Natural rate is " + rateInSecs + " per second but desired rate is " +
                                    desiredRatePerSec + ", sleeping for " + sleepTime + " ms to compensate.");
                        }
                        time.sleep(sleepTime);
                    }
                }
                periodStartNs = now;
                observedSoFar = 0.0;
            }
        } finally {
            lock.unlock();
        }
    }
}