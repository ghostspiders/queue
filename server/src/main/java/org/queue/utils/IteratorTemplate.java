package org.queue.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

// 泛型迭代器模板
public abstract class IteratorTemplate<T> implements Iterator<T> {

    private State state = State.NOT_READY;
    private T nextItem;

    @Override
    public boolean hasNext() {
        if (state == State.FAILED) {
            throw new IllegalStateException("Iterator is in failed state");
        }
        switch (state) {
            case DONE:
                return false;
            case READY:
                return true;
            default:
                return maybeComputeNext();
        }
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        state = State.NOT_READY;
        if (nextItem == null) {
            throw new IllegalStateException("Expected item but none found.");
        }
        T item = nextItem;
        nextItem = null; // Reset the nextItem
        return item;
    }

    // 需要被子类实现，以提供迭代器的下一个元素
    protected abstract T makeNext() throws Throwable;

    private boolean maybeComputeNext() {
        state = State.FAILED;
        nextItem = makeNext();
        if (state == State.DONE) {
            return false;
        } else {
            state = State.READY;
            return true;
        }
    }

    // 需要被子类实现，以确定迭代器是否完成
    protected T allDone() {
        state = State.DONE;
        return null;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removal not supported");
    }
}