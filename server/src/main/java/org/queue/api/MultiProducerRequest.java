package org.queue.api;

import org.queue.network.Request;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MultiProducerRequest extends Request {
    private List<ProducerRequest> produces;

    public MultiProducerRequest(ProducerRequest[] produces) {
        super(RequestKeys.multiProduce);
        this.produces = new ArrayList<>(Arrays.asList(produces));
    }

    public static MultiProducerRequest readFrom(ByteBuffer buffer) {
        int count = buffer.getShort() & 0xFFFF; // Convert short to int and apply mask to handle sign extension
        List<ProducerRequest> producesList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            producesList.add(ProducerRequest.readFrom(buffer));
        }
        return new MultiProducerRequest(producesList.toArray(new ProducerRequest[0]));
    }

    public void writeTo(ByteBuffer buffer) {
        if (produces.size() > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Number of requests in MultiProducer exceeds " + Short.MAX_VALUE + ".");
        }
        buffer.putShort((short) produces.size());
        for (ProducerRequest produce : produces) {
            produce.writeTo(buffer);
        }
    }

    public int sizeInBytes() {
        int size = 2; // Size of the short count
        for (ProducerRequest produce : produces) {
            size += produce.sizeInBytes();
        }
        return size;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        for (ProducerRequest produce : produces) {
            buffer.append(produce.toString());
            buffer.append(",");
        }
        // Remove the last comma if there are any produces
        if (produces.size() > 0) {
            buffer.deleteCharAt(buffer.length() - 1);
        }
        return "MultiProducerRequest{" + buffer.toString() + "}";
    }

    public List<ProducerRequest> getProduces() {
        return produces;
    }
}
