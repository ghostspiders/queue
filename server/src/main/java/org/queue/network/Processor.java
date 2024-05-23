package org.queue.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Processor extends AbstractServerThread {
    private final Handler.HandlerMapping handlerMapping; // 处理器映射，用于将请求类型映射到处理器
    private final Time time; // 时间提供者，用于获取当前时间等
    private final SocketServerStats stats; // 服务器统计信息，用于记录服务器运行时的统计数据
    private final Queue<SocketChannel> newConnections; // 用于存储新连接的队列
    private final Logger requestLogger; // 请求日志记录器，用于记录处理请求的日志

    public Processor(Handler.HandlerMapping handlerMapping, Time time, SocketServerStats stats) {
        this.handlerMapping = handlerMapping;
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
                        } catch (EOFException e) {
                            // 到达文件结束符，关闭连接
                            logger.info("关闭 " + channelFor(key).socket().getInetAddress() + " 的套接字。");
                            close(key);
                        } catch (Throwable e) {
                            // 其他异常，关闭连接并记录错误
                            logger.info("因为错误关闭 " + channelFor(key).socket().getInetAddress() + " 的套接字");
                            logger.error(e.getMessage(), e);
                            close(key);
                        }
                    }
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "选择器选择失败", e);
            }
        }
        logger.debug("关闭选择器。");
        Utils.swallow(Level.INFO, selector::close); // 关闭选择器并忽略异常
        shutdownComplete(); // 标记关闭完成
    }

    private void close(SelectionKey key) {
        // 关闭选择键关联的通道
        SocketChannel channel = (SocketChannel) key.channel;
        if (logger.isDebugEnabled()) {
            logger.debug("关闭来自 " + channel.socket().getRemoteSocketAddress() + " 的连接。");
        }
        Utils.swallow(Level.INFO, channel::socket); // 关闭套接字并忽略异常
        Utils.swallow(Level.INFO, channel::close); // 关闭通道并忽略异常
        key.attach(null); // 将选择键的附件设置为null
        Utils.swallow(Level.INFO, key::cancel); // 取消选择键并忽略异常
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

    // 以下handle、read和write方法的具体实现需要根据业务逻辑来填充
    private Object handle(SelectionKey key, Receive request) {
        // 处理请求并返回响应的方法，需要具体实现
        return null;
    }

    private void read(SelectionKey key) {
        // 读取数据的方法，需要具体实现
    }

    private void write(SelectionKey key) {
        // 写入数据的方法，需要具体实现
    }

    private SocketChannel channelFor(SelectionKey key) {
        // 获取与选择键关联的Socket通道
        return (SocketChannel) key.channel();
    }
}