package org.queue.utils;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class SnapshotStats {
    // 默认监控持续时间为600秒
    private final long monitorDurationNs;
    private final Time time; // Time接口的实例，用于获取纳秒时间戳
    private final AtomicReference<Stats> complete; // 用于存储累积统计信息的引用
    private final AtomicReference<Stats> current; // 用于存储当前统计信息的引用
    private final AtomicLong numCumulatedRequests; // 累计请求数

    // 构造函数，可指定监控持续时间，默认为600秒
    public SnapshotStats(long monitorDurationNs) {
        this.monitorDurationNs = monitorDurationNs;
        this.time = SystemTime.getInstance(); // 使用SystemTime作为时间提供者
        this.complete = new AtomicReference<>(new Stats());
        this.current = new AtomicReference<>(new Stats());
        this.numCumulatedRequests = new AtomicLong(0);
    }
    public SnapshotStats() {
        this(600);
    }

    // 记录请求指标（纳秒）
    public void recordRequestMetric(long requestNs) {
        Stats stats = current.get();
        stats.add(requestNs);
        numCumulatedRequests.incrementAndGet();
        long ageNs = time.nanoseconds() - stats.start;

        // 如果当前统计信息太旧，则进行切换
        if (ageNs >= monitorDurationNs) {
            if (current.compareAndSet(stats, new Stats())) {
                complete.set(stats);
                stats.end.set(time.nanoseconds());
            }
        }
    }

    // 记录吞吐量指标（字节）
    public void recordThroughputMetric(long data) {
        Stats stats = current.get();
        stats.addData(data);
        long ageNs = time.nanoseconds() - stats.start;

        // 如果当前统计信息太旧，则进行切换
        if (ageNs >= monitorDurationNs) {
            if (current.compareAndSet(stats, new Stats())) {
                complete.set(stats);
                stats.end.set(time.nanoseconds());
            }
        }
    }

    // 获取累计请求数
    public long getNumRequests() {
        return numCumulatedRequests.get();
    }

    // 获取每秒请求数
    public double getRequestsPerSecond() {
        Stats stats = complete.get();
        return stats.numRequests / stats.durationSeconds();
    }

    // 获取吞吐量（每秒字节数）
    public double getThroughput() {
        Stats stats = complete.get();
        return stats.totalData / stats.durationSeconds();
    }

    // 获取平均请求指标
    public double getAvgMetric() {
        Stats stats = complete.get();
        return stats.numRequests == 0 ? 0 : (double) stats.totalRequestMetric / stats.numRequests;
    }

    // 获取最大请求指标
    public double getMaxMetric() {
        return complete.get().maxRequestMetric;
    }

    // 内部类Stats，用于存储统计数据
    private class Stats {
        long start; // 开始时间（纳秒）
        AtomicLong end; // 结束时间（纳秒）
        int numRequests; // 请求数
        long totalRequestMetric; // 请求指标总和
        long maxRequestMetric; // 最大请求指标
        long totalData; // 传输的总数据量（字节）

        // 构造函数，初始化开始时间和结束时间为-1
        public Stats() {
            this.start = time.nanoseconds();
            this.end = new AtomicLong(-1);
        }

        // 添加数据量
        public void addData(long data) {
            synchronized (this) {
                totalData += data;
            }
        }

        // 添加请求指标
        public void add(long requestNs) {
            synchronized (this) {
                numRequests++;
                totalRequestMetric += requestNs;
                maxRequestMetric = Math.max(maxRequestMetric, requestNs);
            }
        }

        // 持续时间（秒）
        public double durationSeconds() {
            return (end.get() - start) / (1000.0 * 1000.0 * 1000.0);
        }

        // 持续时间（毫秒）
        public double durationMs() {
            return (end.get() - start) / (1000.0 * 1000.0);
        }
    }
}