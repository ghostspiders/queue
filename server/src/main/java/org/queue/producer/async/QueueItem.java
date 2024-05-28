package org.queue.producer.async;

public class QueueItem<T> {
    private T data;
    private String topic;
    private int partition;

    public QueueItem(T data, String topic, int partition) {
        this.data = data;
        this.topic = topic;
        this.partition = partition;
    }

    public T getData() {
        return data;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public String toString() {
        return "topic: " + topic + ", partition: " + partition + ", data: " + data.toString();
    }
}