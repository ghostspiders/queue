package org.queue.consumer;

public class SimpleConsumerStats implements SimpleConsumerStatsMBean {
    private SnapshotStats fetchRequestStats = new SnapshotStats(); // 假设SnapshotStats类已在Java中定义

    public void recordFetchRequest(long requestNs) {
        fetchRequestStats.recordRequestMetric(requestNs);
    }

    public void recordConsumptionThroughput(long data) {
        fetchRequestStats.recordThroughputMetric(data);
    }

    @Override
    public double getFetchRequestsPerSecond() {
        return fetchRequestStats.getRequestsPerSecond();
    }

    @Override
    public double getAvgFetchRequestMs() {
        return fetchRequestStats.getAvgMetric() / (1000.0 * 1000.0);
    }

    @Override
    public double getMaxFetchRequestMs() {
        return fetchRequestStats.getMaxMetric() / (1000.0 * 1000.0);
    }

    @Override
    public long getNumFetchRequests() {
        return fetchRequestStats.getNumRequests();
    }

    @Override
    public double getConsumerThroughput() {
        return fetchRequestStats.getThroughput();
    }
}
