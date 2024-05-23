package org.queue.consumer;

public interface SimpleConsumerStatsMBean {

    double getFetchRequestsPerSecond();
    double getAvgFetchRequestMs();
    double getMaxFetchRequestMs();
    long getNumFetchRequests();
    double getConsumerThroughput();
}