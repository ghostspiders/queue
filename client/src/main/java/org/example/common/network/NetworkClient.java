package org.example.common.network;

import org.example.ClientRequest;
import org.example.ClientResponse;

import java.io.IOException;
import java.util.List;

/**
 * @author gaoyvfeng
 * @ClassName NetworkClient
 * @description:
 * @datetime 2024年 01月 31日 17:50
 * @version: 1.0
 */
public class NetworkClient implements QueueClient{

    public NetworkClient(){

    }



    /**
     * 是否已经连接给定节点
     *
     * @param node
     * @param now
     */
    @Override
    public boolean isReady(Node node, long now) {
        return false;
    }

    /**
     * 连接给定节点
     *
     * @param node
     * @param now
     */
    @Override
    public boolean ready(Node node, long now) {
        return false;
    }

    /**
     * 根据连接状态检查节点的连接是否失败。这种连接故障是通常是暂时的，
     *
     * @param node
     */
    @Override
    public boolean connectionFailed(Node node) {
        return false;
    }

    @Override
    public void send(ClientRequest request, long now) {

    }

    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        return null;
    }

    /**
     * 断开与特定节点的连接
     *
     * @param nodeId
     */
    @Override
    public void disconnect(String nodeId) {

    }

    /**
     * 关闭与特定节点的连接
     *
     * @param nodeId
     */
    @Override
    public void close(String nodeId) {

    }

    /**
     * Wake up the client if it is currently blocked waiting for I/O
     */
    @Override
    public void wakeup() {

    }

    /**
     * 客户端是否存活
     */
    @Override
    public boolean active() {
        return false;
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {

    }
}
