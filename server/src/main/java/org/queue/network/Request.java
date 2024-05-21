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
    private final int id;


    public Request(int id) {
        this.id = id;
    }


    public int getId() {
        return id;
    }


    public abstract int sizeInBytes();


    public abstract void writeTo(ByteBuffer buffer);
}
