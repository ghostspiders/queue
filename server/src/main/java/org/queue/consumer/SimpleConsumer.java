package org.queue.consumer;

/**
 * @author gaoyvfeng
 * @ClassName SimpleConsumer
 * @description:
 * @datetime 2024年 05月 24日 09:47
 * @version: 1.0
 */
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleConsumer {
    // 服务器地址
    private final String host;
    // 服务器端口
    private final int port;
    // 套接字超时时间
    private final int soTimeout;
    // 缓冲区大小
    private final int bufferSize;
    // 套接字通道
    private SocketChannel channel  = null;
    // 锁对象，用于同步
    private final Object lock = new Object();
    // 日志记录器
    private static final Logger logger = Logger.getLogger(SimpleConsumer.class.getName());

    // 构造函数，初始化消费者
    public SimpleConsumer(String host, int port, int soTimeout, int bufferSize) {
        this.host = host;
        this.port = port;
        this.soTimeout = soTimeout;
        this.bufferSize = bufferSize;
    }

    // 建立连接
    private SocketChannel connect() {
        synchronized (lock) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    logger.log(Level.WARNING, "关闭套接字通道失败", e);
                }
            }

            try {
                SocketChannel newChannel = SocketChannel.open();
                newChannel.configureBlocking(true);
                newChannel.socket().setReceiveBufferSize(bufferSize);
                newChannel.socket().setSoTimeout(soTimeout);
                newChannel.socket().setKeepAlive(true);
                newChannel.connect(new InetSocketAddress(host, port));
                logger.info("成功连接到 " + host + ":" + port + " 进行获取数据.");
                channel = newChannel;
                return channel;
            } catch (IOException e) {
                logger.log(Level.SEVERE, "连接到 " + host + ":" + port + " 失败", e);
            }
        }
        return null;
    }

    // 关闭消费者资源
    public void close() {
        synchronized (lock) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    logger.log(Level.WARNING, "关闭套接字通道失败", e);
                }
                channel = null;
            }
        }
    }

    // 从指定主题获取一组消息
    public ByteBufferMessageSet fetch(FetchRequest request) {
        synchronized (lock) {
            long startTime = System.nanoTime();
            getOrMakeConnection();
            try {
                sendRequest(request);
                ByteBuffer buffer = receiveResponse();
                return new ByteBufferMessageSet(buffer, request.getOffset(), getResponseSize(buffer));
            } catch (IOException e) {
                logger.info("获取失败，由于: " + e);
                channel = connect();
                sendRequest(request);
                ByteBuffer buffer = receiveResponse();
                return new ByteBufferMessageSet(buffer, request.getOffset(), getResponseSize(buffer));
            }
        }
    }

    // 组合多个获取请求到一个调用中
    public MultiFetchResponse multifetch(FetchRequest[] fetches) {
        synchronized (lock) {
            long startTime = System.nanoTime();
            getOrMakeConnection();
            try {
                sendRequest(new MultiFetchRequest(fetches));
                ByteBuffer buffer = receiveResponse();
                return new MultiFetchResponse(buffer, fetches.length, getOffsetsArray(buffer));
            } catch (IOException e) {
                logger.info("多获取失败，由于: " + e);
                channel = connect();
                sendRequest(new MultiFetchRequest(fetches));
                ByteBuffer buffer = receiveResponse();
                return new MultiFetchResponse(buffer, fetches.length, getOffsetsArray(buffer));
            }
        }
    }

    // 获取给定时间之前的有效偏移量列表
    public long[] getOffsetsBefore(String topic, int partition, long time, int maxNumOffsets) {
        synchronized (lock) {
            getOrMakeConnection();
            try {
                sendRequest(new OffsetRequest(topic, partition, time, maxNumOffsets));
                ByteBuffer buffer = receiveRequest();
                return OffsetRequest.deserializeOffsetArray(buffer);
            } catch (IOException e) {
                logger.info("获取偏移量失败，由于: " + e);
                channel = connect();
                sendRequest(new OffsetRequest(topic, partition, time, maxNumOffsets));
                ByteBuffer buffer = receiveRequest();
                return OffsetRequest.deserializeOffsetArray(buffer);
            }
        }
    }

    /**
     * 发送请求到服务器。
     * @param request 请求对象
     */
    private void sendRequest(Request request) {
        BoundedByteBufferSend send = new BoundedByteBufferSend(request);
        try {
            send.writeCompletely(channel); // 将请求完全写入通道
        } catch (IOException e) {
            // 日志记录或异常处理
        }
    }

    /**
     * 从服务器接收响应。
     * @return 包含接收到的数据和错误码的元组
     */
    private Tuple2<Receive, Integer> getResponse() {
        BoundedByteBufferReceive response = new BoundedByteBufferReceive();
        try {
            response.readCompletely(channel); // 从通道完全读取响应

            // 设置buffer的初始位置
            ByteBuffer buffer = response.getBuffer();
            int errorCode = buffer.getShort(); // 获取错误码
            return new Tuple2<>(response, errorCode); // 返回接收对象和错误码
        } catch (IOException e) {
            // 日志记录或异常处理
            return null;
        }
    }

    /**
     * 确保已经建立连接，如果没有则新建连接。
     */
    private void getOrMakeConnection() {
        if (channel == null || !channel.isConnected()) {
            channel = connect(); // 如果没有连接，则调用connect()新建连接
        }
    }
}