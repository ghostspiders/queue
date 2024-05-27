package org.queue.client;

public class QueueConsumerProducerDemo implements QueueProperties {
    public static void main(String[] args) {
        Producer producerThread = new Producer(QueueProperties.topic);
        producerThread.start();

        Consumer consumerThread = new Consumer(QueueProperties.topic);
        consumerThread.start();
    }
}

