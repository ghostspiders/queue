package org.queue.network;

import java.nio.ByteBuffer;
/**
 * @author gaoyvfeng
 * @ClassName Request
 * @description:
 * @datetime 2024年 05月 21日 17:26
 * @version: 1.0
 */


public abstract class Request {
    private final short id;


    public Request(short id) {
        this.id = id;
    }


    public short getId() {
        return id;
    }


    public abstract int sizeInBytes();


    public abstract void writeTo(ByteBuffer buffer);
}
