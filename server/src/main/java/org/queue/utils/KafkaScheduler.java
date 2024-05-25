package org.queue.utils;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;
import java.util.concurrent.Executors;

/**
 * 用于后台运行任务的调度器
 * 注意：ScheduledThreadPoolExecutor 臭名昭著地会吞掉异常
 */
public class KafkaScheduler {
    private final int numThreads;
    private final String baseThreadName;
    private final boolean isDaemon;
    private final Logger logger;
    private final AtomicLong threadId;
    private final ScheduledThreadPoolExecutor executor;

    public KafkaScheduler(int numThreads, String baseThreadName, boolean isDaemon) {
        this.numThreads = numThreads;
        this.baseThreadName = baseThreadName;
        this.isDaemon = isDaemon;
        this.logger = Logger.getLogger(KafkaScheduler.class);
        this.threadId = new AtomicLong(0);
        this.executor = new ScheduledThreadPoolExecutor(numThreads, new ThreadFactory());
    }

    /**
     * 按照给定的延迟和周期安排任务
     * @param fun 要执行的任务
     * @param delayMs 延迟时间，单位为毫秒
     * @param periodMs 周期时间，单位为毫秒
     */
    public void scheduleWithRate(Runnable fun, long delayMs, long periodMs) {
        executor.scheduleAtFixedRate(Utils.loggedRunnable(fun), delayMs, periodMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 关闭调度器
     */
    public void shutdown() {
        executor.shutdownNow();
        logger.info("shutdown scheduler " + baseThreadName);
    }

    /**
     * 自定义线程工厂，用于设置线程名称和守护线程状态
     */
    private class ThreadFactory implements java.util.concurrent.ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName(baseThreadName + threadId.getAndIncrement());
            t.setDaemon(isDaemon);
            return t;
        }
    }
}
