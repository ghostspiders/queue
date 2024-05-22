package org.queue.api;

import org.queue.message.ByteBufferMessageSet;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;

public class MultiFetchResponse implements Iterable<ByteBufferMessageSet> {
    private ByteBuffer buffer;
    private int numSets;
    private long[] offsets;
    private List<ByteBufferMessageSet> messageSets;

    public MultiFetchResponse(ByteBuffer buffer, int numSets, long[] offsets) {
        this.buffer = buffer;
        this.numSets = numSets;
        this.offsets = offsets;
        this.messageSets = new ArrayList<ByteBufferMessageSet>();
        initializeMessageSets();
    }

    private void initializeMessageSets() {
        for (int i = 0; i < numSets; i++) {
            int size = buffer.getInt();
            int errorCode = buffer.getShort();
            ByteBuffer copy = buffer.slice();
            int payloadSize = size - 2;
            copy.limit(payloadSize);
            buffer.position(buffer.position() + payloadSize);
            messageSets.add(new ByteBufferMessageSet(copy, offsets[i], errorCode));
        }
    }

    @Override
    public Iterator<ByteBufferMessageSet> iterator() {
        return messageSets.iterator();
    }

    @Override
    public String toString() {
        return messageSets.toString();
    }
}