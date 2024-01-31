package org.example.common.network;

import org.example.ClientRequest;
import org.example.ClientResponse;

import java.io.Closeable;
import java.util.List;

/**
 * @author gaoyvfeng
 * @ClassName QueueClient
 * @description:
 * @datetime 2024年 01月 31日 16:38
 * @version: 1.0
 */
public interface QueueClient extends Closeable {
    /**
     * 是否已经连接给定节点
     */
    boolean isReady(Node node, long now);

    /**
     * 连接给定节点
     */
    boolean ready(Node node, long now);



    /**
     * 根据连接状态检查节点的连接是否失败。这种连接故障是通常是暂时的，
     */
    boolean connectionFailed(Node node);


    void send(ClientRequest request, long now);


    List<ClientResponse> poll(long timeout, long now);

    /**
     * 断开与特定节点的连接
     */
    void disconnect(String nodeId);

    /**
     * 关闭与特定节点的连接
     */
    void close(String nodeId);


    /**
     * Wake up the client if it is currently blocked waiting for I/O
     */
    void wakeup();

    /**
     * 客户端是否存活
     */
    boolean active();

}
