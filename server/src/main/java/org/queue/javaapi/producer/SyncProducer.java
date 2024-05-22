package org.queue.javaapi.producer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.queue.api.ProducerRequest;
import org.queue.javaapi.ProducerRequest;
import org.queue.message.ByteBufferMessageSet;
import org.queue.producer.SyncProducer;

public class SyncProducer {
    private final SyncProducer underlying;

    public SyncProducer(SyncProducer syncProducer) {
        this.underlying = syncProducer;
    }

    /**
     * Sends the given messages to the specified topic and partition.
     *
     * @param topic      The topic to which messages are sent.
     * @param partition  The partition to which messages are sent.
     * @param messages   The messages to send.
     */
    public void send(String topic, int partition, ByteBufferMessageSet messages) {
        underlying.send(topic, partition, messages);
    }

    /**
     * Sends the given messages to the specified topic using a random partition.
     *
     * @param topic      The topic to which messages are sent.
     * @param messages   The messages to send.
     */
    public void send(String topic, ByteBufferMessageSet messages) {
        send(topic, ProducerRequest.RandomPartition, messages);
    }

    /**
     * Sends multiple messages to various topics and partitions.
     *
     * @param produces   An array of ProducerRequest objects containing topic, partition, and messages.
     */
    public void multiSend(ProducerRequest[] produces) {
        org.queue.api.ProducerRequest[] produceRequests = new org.queue.api.ProducerRequest[produces.length];
        for (int i = 0; i < produces.length; i++) {
            produceRequests[i] = new org.queue.api.ProducerRequest(
                    produces[i].getTopic(),
                    produces[i].getPartition(),
                    produces[i].getMessages()
            );
        }
        underlying.multiSend(produceRequests);
    }

    /**
     * Closes the underlying SyncProducer and releases all resources.
     */
    public void close() {
        underlying.close();
    }
}