package org.queue.network;

import java.util.concurrent.TimeUnit;

/**
 * 服务器统计信息的Java类，实现了SocketServerStatsMBean接口。
 */
public class SocketServerStats implements SocketServerStatsMBean {
    private final long monitorDurationNs; // 监控持续时间，单位纳秒
    private final Time time; // 时间提供者，用于获取当前时间等
    private SnapshotStats produceTimeStats; // 生产请求处理时间统计
    private SnapshotStats fetchTimeStats; // 获取请求处理时间统计
    private SnapshotStats produceBytesStats; // 生产请求读写字节统计
    private SnapshotStats fetchBytesStats; // 获取请求读写字节统计

    // 构造函数，接收监控持续时间和时间提供者
    public SocketServerStats(long monitorDurationNs, Time time) {
        this.monitorDurationNs = monitorDurationNs;
        this.time = time;
        this.produceTimeStats = new SnapshotStats(monitorDurationNs);
        this.fetchTimeStats = new SnapshotStats(monitorDurationNs);
        this.produceBytesStats = new SnapshotStats(monitorDurationNs);
        this.fetchBytesStats = new SnapshotStats(monitorDurationNs);
    }

    // 无参构造函数，使用默认的监控持续时间，时间提供者为SystemTime
    public SocketServerStats(long monitorDurationNs) {
        this(monitorDurationNs, SystemTime.INSTANCE);
    }

    // 记录请求的处理时间和类型
    public void recordRequest(short requestTypeId, long durationNs) {
        switch (requestTypeId) {
            case RequestKeys.PRODUCE:
            case RequestKeys.MULTI_PRODUCE:
                produceTimeStats.recordRequestMetric(durationNs);
                break;
            case RequestKeys.FETCH:
            case RequestKeys.MULTI_FETCH:
                fetchTimeStats.recordRequestMetric(durationNs);
                break;
            default:
                // 不收集此类型的请求数据
                break;
        }
    }

    // 记录写入的字节数
    public void recordBytesWritten(int bytes) {
        fetchBytesStats.recordRequestMetric(bytes);
    }

    // 记录读取的字节数
    public void recordBytesRead(int bytes) {
        produceBytesStats.recordRequestMetric(bytes);
    }

    // 实现SocketServerStatsMBean接口中定义的方法
    @Override
    public double getProduceRequestsPerSecond() {
        return produceTimeStats.getRequestsPerSecond();
    }

    @Override
    public double getFetchRequestsPerSecond() {
        return fetchTimeStats.getRequestsPerSecond();
    }

    @Override
    public double getAvgProduceRequestMs() {
        return produceTimeStats.getAvgMetric() / TimeUnit.NANOSECONDS.toMillis(1);
    }

    @Override
    public double getMaxProduceRequestMs() {
        return produceTimeStats.getMaxMetric() / TimeUnit.NANOSECONDS.toMillis(1);
    }

    @Override
    public double getAvgFetchRequestMs() {
        return fetchTimeStats.getAvgMetric() / TimeUnit.NANOSECONDS.toMillis(1);
    }

    @Override
    public double getMaxFetchRequestMs() {
        return fetchTimeStats.getMaxMetric() / TimeUnit.NANOSECONDS.toMillis(1);
    }

    @Override
    public double getBytesReadPerSecond() {
        return produceBytesStats.getAvgMetric();
    }

    @Override
    public double getBytesWrittenPerSecond() {
        return fetchBytesStats.getAvgMetric();
    }

    @Override
    public long getNumFetchRequests() {
        return fetchTimeStats.getNumRequests();
    }

    @Override
    public long getNumProduceRequests() {
        return produceTimeStats.getNumRequests();
    }
}
