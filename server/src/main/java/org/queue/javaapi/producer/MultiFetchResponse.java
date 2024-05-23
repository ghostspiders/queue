package org.queue.javaapi.producer;

import org.queue.message.ByteBufferMessageSet;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class MultiFetchResponse implements Iterable<ByteBufferMessageSet> {
    private final ByteBuffer underlyingBuffer;
    private final org.queue.api.MultiFetchResponse underlying;
    private final Iterator<ByteBufferMessageSet> iterator;

    public MultiFetchResponse(ByteBuffer buffer, int numSets, Long[] offsets) {
        // Wraps the buffer and sets the initial position
        this.underlyingBuffer = ByteBuffer.wrap(buffer.array());
        this.underlying = new org.queue.api.MultiFetchResponse(underlyingBuffer, numSets, offsets);
        this.iterator = new IteratorTemplate<>();
    }

    @Override
    public String toString() {
        return underlying.toString();
    }

    @Override
    public Iterator<ByteBufferMessageSet> iterator() {
        return iterator;
    }

    private class IteratorTemplate<ByteBufferMessageSet> implements Iterator<ByteBufferMessageSet> {
        private final Iterator<ByteBufferMessageSet> iter = underlying.iterator();

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public ByteBufferMessageSet next() {
            if (hasNext()) {
                return iter.next();
            } else {
                throw new NoSuchElementException("No more elements");
            }
        }
    }
}