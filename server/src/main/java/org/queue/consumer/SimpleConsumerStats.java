package org.queue.consumer;

import org.queue.utils.SnapshotStats;

public class SimpleConsumerStats implements SimpleConsumerStatsMBean {
    private static SnapshotStats fetchRequestStats = new SnapshotStats();

    public static void recordFetchRequest(long requestNs) {
        fetchRequestStats.recordRequestMetric(requestNs);
    }

    public static void recordConsumptionThroughput(long data) {
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
