package org.queue.producer;

/**
 * @author gaoyvfeng
 * @ClassName SyncProducer
 * @description:
 * @datetime 2024年 05月 24日 17:43
 * @version: 1.0
 */
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SyncProducer {
    // 同步生产者配置
    private final SyncProducerConfig config;
    // 日志记录器
    private final Logger logger = Logger.getLogger(SyncProducer.class.getName());
    // 最大连接回退时间（毫秒）
    private static final int MaxConnectBackoffMs = 60000;
    // 用于发送数据的SocketChannel
    private SocketChannel channel = null;
    // 已发送数据的次数
    private int sentOnConnection = 0;
    // 锁对象，用于同步操作
    private final Object lock = new Object();
    // 标记是否已关闭
    private volatile boolean shutdown = false;

    public SyncProducer(SyncProducerConfig config) {
        this.config = config;
        // 记录实例化信息
        logger.fine("Instantiating Java Sync Producer");
    }

    /**
     * 验证发送缓冲区。
     * @param buffer 要验证的ByteBuffer对象。
     */
    private void verifySendBuffer(ByteBuffer buffer) {
        if (logger.isLoggable(Level.FINER)) { // 检查是否启用了追踪日志
            logger.finer("verifying send buffer of size " + buffer.limit()); // 记录缓冲区大小
            short requestTypeId = buffer.getShort(); // 获取请求类型ID
            if (requestTypeId == RequestKeys.MultiProduce) { // 检查是否为多产生请求
                try {
                    MultiProducerRequest request = MultiProducerRequest.readFrom(buffer); // 从ByteBuffer中读取请求
                    for (Produce produce : request.produces) { // 遍历每个产生请求
                        try {
                            for (MessageAndOffset messageAndOffset : produce.messages) { // 遍历消息
                                if (!messageAndOffset.message.isValid()) { // 检查消息是否有效
                                    logger.finer("topic " + produce.topic + " is invalid"); // 记录无效主题
                                }
                            }
                        } catch (Throwable e) {
                            // 记录消息迭代中的错误
                            logger.log(Level.FINER, "error iterating messages ", e);
                        }
                    }
                } catch (Throwable e) {
                    // 记录验证发送缓冲区中的错误
                    logger.log(Level.FINER, "error verifying send buffer ", e);
                }
            }
        }
    }

    // 公共发送逻辑
    private void send(BoundedByteBufferSend send) {
        synchronized (lock) {
            verifySendBuffer(send.getBuffer().slice());
            long startTime = System.nanoTime(); // 发送开始时间
            getOrMakeConnection(); // 获取或创建连接

            try {
                send.writeCompletely(channel); // 完全写入数据
            } catch (IOException e) {
                // 发生IO异常，断开连接并重新抛出异常
                disconnect();
                throw e;
            } catch (Throwable e2) {
                throw e2;
            }
            // 更新发送计数
            sentOnConnection++;
            // 达到重连间隔后断开并重新连接
            if (sentOnConnection >= config.getReconnectInterval()) {
                disconnect();
                channel = connect();
                sentOnConnection = 0;
            }
            long endTime = System.nanoTime(); // 发送结束时间
            // 记录发送请求耗时
            SyncProducerStats.recordProduceRequest(endTime - startTime);
        }
    }

    // 发送消息到指定主题和分区
    public void send(String topic, int partition, ByteBufferMessageSet messages) {
        verifyMessageSize(messages); // 验证消息大小
        int setSize = (int) messages.sizeInBytes(); // 设置消息大小
        if (logger.isLoggable(Level.FINE)) {
            // 记录消息大小日志
            logger.fine("Got message set with " + setSize + " bytes to send");
        }
        send(new BoundedByteBufferSend(new ProducerRequest(topic, partition, messages))); // 发送请求
    }

    // 发送消息到随机分区
    public void send(String topic, ByteBufferMessageSet messages) {
        send(topic, ProducerRequest.RandomPartition, messages);
    }

    // 批量发送消息
    public void multiSend(ProducerRequest[] produces) {
        for (ProducerRequest request : produces) {
            verifyMessageSize(request.getMessages()); // 验证每个请求的消息大小
        }
        long setSize = Arrays.stream(produces).mapToInt(p -> (int) p.getMessages().sizeInBytes()).sum(); // 计算总大小
        if (logger.isLoggable(Level.FINE)) {
            // 记录总消息大小日志
            logger.fine("Got multi message sets with " + setSize + " bytes to send");
        }
        send(new BoundedByteBufferSend(new MultiProducerRequest(produces))); // 发送批量请求
    }

    // 关闭生产者资源
    public void close() {
        synchronized (lock) {
            disconnect(); // 断开连接
            shutdown = true; // 设置关闭标记
        }
    }

    /**
     * 验证消息集合中每个消息的大小是否超过配置的最大消息大小。
     * @param messages 要验证的消息集合。
     */
    private void verifyMessageSize(ByteBufferMessageSet messages) {
        for (MessageAndOffset messageAndOffset : messages) {
            if (messageAndOffset.getMessage().payloadSize() > config.getMaxMessageSize()) {
                throw new MessageSizeTooLargeException("Message size exceeds maximum configured size");
            }
        }
    }

    /**
     * 从当前通道断开连接，并关闭连接。
     * 副作用：成功断开连接后，channel字段将被设置为null。
     */
    private void disconnect() {
        try {
            if (channel != null) {
                logger.log(Level.INFO, "Disconnecting from {0}:{1}", new Object[]{config.getHost(), config.getPort()});
                Utils.swallow(Level.WARN, () -> channel.close());
                Socket socket = channel.socket();
                Utils.swallow(Level.WARN, () -> socket.close());
                channel = null; // 设置为null，表示已断开连接
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error on disconnect: ", e);
        }
    }
    /**
     * 建立到指定地址的Socket连接。
     * @return 建立的SocketChannel对象。
     */
    private SocketChannel connect() {
        SocketChannel channel = null;
        int connectBackoffMs = 1;
        long beginTimeMs = System.currentTimeMillis();
        while (channel == null && !shutdown) {
            try {
                channel = SocketChannel.open();
                Socket socket = channel.socket();
                socket.setSendBufferSize(config.getBufferSize());
                socket.setKeepAlive(true);
                socket.setSoTimeout(config.getSocketTimeoutMs());
                channel.configureBlocking(true);
                channel.connect(new InetSocketAddress("127.0.0.1", config.getPort()));
                logger.info("Connected to " + config.getHost() + ":" + config.getPort() + " for producing");
            } catch (IOException e) {
                disconnect();
                long endTimeMs = System.currentTimeMillis();
                if ((endTimeMs - beginTimeMs + connectBackoffMs) > config.getConnectTimeoutMs()) {
                    logger.log(Level.SEVERE, "Producer connection timing out after " + config.getConnectTimeoutMs() + " ms", e);
                    throw new RuntimeException("Failed to connect to the server", e);
                }
                logger.log(Level.SEVERE, "Connection attempt failed, next attempt in " + connectBackoffMs + " ms", e);
                try {
                    Thread.sleep(connectBackoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                connectBackoffMs = Math.min(connectBackoffMs * 10, MaxConnectBackoffMs);
            }
        }
        return channel;
    }

    /**
     * 获取现有的连接或创建新连接。
     */
    private void getOrMakeConnection() {
        if (channel == null) {
            channel = connect();
        }
    }
}