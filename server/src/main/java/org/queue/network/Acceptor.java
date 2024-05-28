package org.queue.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;

public class Acceptor extends AbstractServerThread {

    private final int port; // 监听端口
    private final Processor[] processors; // 处理器数组

    public Acceptor(int port, Processor[] processors) throws IOException {
        super();
        this.port = port;
        this.processors = processors;
    }

    /**
     * 运行接受器线程，检查新的连接请求。
     */
    @Override
    public void run() {
        ServerSocketChannel serverChannel = null;
        try {
            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);
            serverChannel.socket().bind(new InetSocketAddress(port));
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            logger.info("Awaiting connections on port " + port);
            startupComplete();

            int currentProcessor = 0;
            while (isRunning()) {
                try {
                    int ready = selector.select(500);
                    if (ready > 0) {
                        Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                        while (keys.hasNext() && isRunning()) {
                            SelectionKey key = null;
                            try {
                                key = keys.next();
                                keys.remove();

                                if (key.isAcceptable()) {
                                    accept(key, processors[currentProcessor]);
                                } else {
                                    throw new IllegalStateException("Unrecognized key state for acceptor thread.");
                                }

                                // Round-robin to the next processor thread
                                currentProcessor = (currentProcessor + 1) % processors.length;
                            } catch (Exception e) {
                                logger.error("Error in acceptor", e);
                            }
                        }
                    }
                } catch (IOException e) {
                    logger.error("Selector select failed", e);
                }
            }
        } catch (Exception e) {
            logger.error("Error in acceptor thread", e);
        } finally {
            logger.debug("Closing server socket and selector.");
            try {
                serverChannel.close();
            } catch (IOException e) {
                // Ignore
            }
            try {
                selector.close();
            } catch (IOException e) {
                // Ignore
            }
            shutdownComplete();
        }
    }

    /**
     * 接受一个新的连接。
     */
    private void accept(SelectionKey key, Processor processor) throws IOException {
        SocketChannel socketChannel = ((ServerSocketChannel) key.channel()).accept();
        if (logger.isInfoEnabled()) {
            logger.info("Accepted connection from " + socketChannel.socket().getInetAddress() +
                    " on " + socketChannel.socket().getLocalSocketAddress());
        }
        socketChannel.configureBlocking(false);
        socketChannel.socket().setTcpNoDelay(true);
        processor.accept(socketChannel);
    }
}