package org.queue.network;

import java.io.IOException;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.queue.api.RequestKeys;
import org.queue.server.QueueRequestHandlers;
import org.queue.utils.Time;
import org.queue.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public class Processor extends AbstractServerThread {
    private final QueueRequestHandlers handlerFactory; // 处理器映射，用于将请求类型映射到处理器
    private final Time time; // 时间提供者，用于获取当前时间等
    private final SocketServerStats stats; // 服务器统计信息，用于记录服务器运行时的统计数据
    private final Queue<SocketChannel> newConnections; // 用于存储新连接的队列
    private final Logger requestLogger; // 请求日志记录器，用于记录处理请求的日志

    public Processor(QueueRequestHandlers handlerFactory, Time time, SocketServerStats stats) throws IOException {
        super();
        this.handlerFactory = handlerFactory;
        this.time = time;
        this.stats = stats;
        this.newConnections = new ConcurrentLinkedQueue<>();
        this.requestLogger = LoggerFactory.getLogger("org.queue.request.logger");
    }

    @Override
    public void run() {
        startupComplete(); // 标记启动完成
        while (isRunning()) {
            // 配置任何新排队的连接
            configureNewConnections();

            try {
                int ready = selector.select(500); // 选择就绪的通道，超时时间为500毫秒
                if (ready > 0) {
                    Set<SelectionKey> keys = selector.selectedKeys(); // 获取选择键集合
                    Iterator<SelectionKey> iter = keys.iterator();
                    while (iter.hasNext() && isRunning()) {
                        SelectionKey key = null;
                        try {
                            key = iter.next();
                            iter.remove(); // 从选中键集合中移除当前键

                            // 根据通道的就绪状态进行操作
                            if (key.isReadable()) {
                                read(key); // 读取数据
                            } else if (key.isWritable()) {
                                write(key); // 写入数据
                            } else if (!key.isValid()) {
                                close(key); // 关闭无效的通道
                            } else {
                                throw new IllegalStateException("处理器线程中无法识别的键状态。");
                            }
                        } catch (Throwable e) {
                            // 其他异常，关闭连接并记录错误
                            logger.info("因为错误关闭 " + channelFor(key).socket().getInetAddress() + " 的套接字");
                            logger.error(e.getMessage(), e);
                            close(key);
                        }
                    }
                }
            } catch (IOException e) {
                logger.error("选择器选择失败", e);
            }
        }
        logger.debug("关闭选择器。");
        try {
            // 关闭选择器并忽略异常
            selector.close();
        } catch (IOException e) {
            Utils.swallow(Level.INFO, e);
        }
        shutdownComplete(); // 标记关闭完成
    }

    private void close(SelectionKey key) {
        // 关闭选择键关联的通道
        SocketChannel channel = (SocketChannel) key.channel();
        if (logger.isDebugEnabled()) {
            logger.debug("关闭来自 " + channel.socket().getRemoteSocketAddress() + " 的连接。");
        }
        try {
            channel.socket().close(); // 关闭套接字并忽略异常
        } catch (IOException e) {
            Utils.swallow(Level.INFO, e);
        }
        try {
            channel.close(); // 关闭通道并忽略异常
        } catch (IOException e) {
            Utils.swallow(Level.INFO, e);
        }
        key.attach(null); // 将选择键的附件设置为null
        key.cancel();// 取消选择键并忽略异常
    }

    public void accept(SocketChannel socketChannel) {
        // 接受新的Socket通道并将其添加到新连接队列
        newConnections.add(socketChannel);
        selector.wakeup(); // 唤醒选择器
    }

    private void configureNewConnections() {
        // 配置新连接
        while (!newConnections.isEmpty()) {
            SocketChannel channel = newConnections.poll();
            if (logger.isDebugEnabled()) {
                logger.debug("监听来自 " + channel.socket().getRemoteSocketAddress() + " 的新连接。");
            }
            try {
                channel.register(selector, SelectionKey.OP_READ); // 注册通道，感兴趣的事件为读
            } catch (ClosedChannelException e) {
                // 忽略异常
            }
        }
    }

    /**
     * 处理已完成的请求并产生一个可选的响应。
     *
     * @param key      SelectionKey对象，代表了一个注册的通道
     * @param request  接收到的请求对象
     * @return 一个可选的Send对象，表示响应
     */
    private Optional<Send> handle(SelectionKey key, Receive request) throws IOException, InterruptedException {
        int requestTypeId = request.getBuffer().getShort();
        if (requestLogger.isTraceEnabled()) {
            switch (requestTypeId) {
                case RequestKeys.produce:
                    requestLogger.trace("Handling produce request from " + channelFor(key).socket().getRemoteSocketAddress());
                    break;
                case RequestKeys.fetch:
                    requestLogger.trace("Handling fetch request from " + channelFor(key).socket().getRemoteSocketAddress());
                    break;
                case RequestKeys.multiFetch:
                    requestLogger.trace("Handling multi-fetch request from " + channelFor(key).socket().getRemoteSocketAddress());
                    break;
                case RequestKeys.multiProduce:
                    requestLogger.trace("Handling multi-produce request from " + channelFor(key).socket().getRemoteSocketAddress());
                    break;
                case RequestKeys.offsets:
                    requestLogger.trace("Handling offset request from " + channelFor(key).socket().getRemoteSocketAddress());
                    break;
                default:
                    throw new InvalidRequestException("No mapping found for handler id " + requestTypeId);
            }
        }
        Optional<Send> send = handlerFactory.handlerFor(requestTypeId, request);
        long start = time.nanoseconds();
        stats.recordRequest(requestTypeId, time.nanoseconds() - start);
        return send;
    }

    /**
     * 从准备好的套接字中读取数据
     */
    public void read(SelectionKey key) throws IOException, InterruptedException {
        SocketChannel socketChannel = channelFor(key);
        Receive request = (Receive) key.attachment();
        if (request == null) {
            request = new BoundedByteBufferReceive();
            key.attach(request);
        }

        int read = request.readFrom(socketChannel);
        stats.recordBytesRead(read);
        if (requestLogger.isTraceEnabled()) {
            requestLogger.trace(read + " bytes read from " + socketChannel.socket().getRemoteSocketAddress());
        }

        if (read < 0) {
            close(key);
        } else if (request.complete()) {
            Optional<Send> maybeResponse = handle(key, request);
            if (maybeResponse.isPresent()) {
                key.attach(maybeResponse.get());
                key.interestOps(SelectionKey.OP_WRITE);
            }
        } else {
            key.interestOps(SelectionKey.OP_READ);
            selector.wakeup();
        }
    }

    /**
     * 向准备好的套接字中写入数据
     */
    public void write(SelectionKey key) throws IOException {
        Send response = (Send) key.attachment();
        SocketChannel socketChannel = channelFor(key);
        int written = response.writeTo(socketChannel);
        stats.recordBytesWritten(written);
        if (requestLogger.isTraceEnabled()) {
            requestLogger.trace(written + " bytes written to " + socketChannel.socket().getRemoteSocketAddress());
        }

        if (response.complete()) {
            key.attach(null);
            key.interestOps(SelectionKey.OP_READ);
        } else {
            key.interestOps(SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    private SocketChannel channelFor(SelectionKey key) {
        return (SocketChannel) key.channel();
    }
}