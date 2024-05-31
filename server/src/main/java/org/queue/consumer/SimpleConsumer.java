package org.queue.consumer;

/**
 * @author gaoyvfeng
 * @ClassName SimpleConsumer
 * @description:
 * @datetime 2024年 05月 24日 09:47
 * @version: 1.0
 */
import cn.hutool.core.map.MapUtil;
import org.queue.api.FetchRequest;
import org.queue.api.MultiFetchRequest;
import org.queue.api.MultiFetchResponse;
import org.queue.api.OffsetRequest;
import org.queue.message.ByteBufferMessageSet;
import org.queue.network.BoundedByteBufferReceive;
import org.queue.network.BoundedByteBufferSend;
import org.queue.network.Receive;
import org.queue.network.Request;
import org.queue.utils.SystemTime;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;

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
    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);

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
                    logger.error( "关闭套接字通道失败", e);
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
                logger.error( "连接到 " + host + ":" + port + " 失败", e);
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
                    logger.error( "关闭套接字通道失败", e);
                }
                channel = null;
            }
        }
    }


    /**
     * 从指定主题获取一组消息。
     *
     * @param request 指定主题名称、分区、起始字节偏移量和要获取的最大字节数。
     * @return 获取到的消息集合
     */
    public ByteBufferMessageSet fetch(FetchRequest request) throws IOException {
        long startTime, endTime;
        ByteBufferMessageSet messageSet;
        Map<Receive, Integer> response;

        synchronized (lock) {
            startTime = SystemTime.nanoseconds(); // 获取开始时间
            getOrMakeConnection(); // 确保已经建立连接
            try {
                sendRequest(request); // 发送请求
                response = getResponse(); // 获取响应
            } catch (IOException e) {
                logger.info("fetch reconnect due to: " + e.getMessage());
                // 重试一次
                try {
                    channel = connect(); // 重新连接
                    sendRequest(request); // 重新发送请求
                    response = getResponse(); // 重新获取响应
                } catch (IOException ioe) {
                    channel = null;
                    throw ioe;
                }
            } catch (Throwable e) {
                throw new RuntimeException("Error during fetch operation", e);
            }

            endTime = SystemTime.nanoseconds(); // 获取结束时间
            SimpleConsumerStats.recordFetchRequest(endTime - startTime); // 记录请求时间

            Map.Entry<Receive, Integer> next = response.entrySet().iterator().next();
            SimpleConsumerStats.recordConsumptionThroughput(next.getKey().getBuffer().limit()); // 记录消费吞吐量
            messageSet = new ByteBufferMessageSet(next.getKey().getBuffer(), request.getOffset(), next.getValue());
        }
        return messageSet;
    }

    /**
     * 组合多个获取请求并在一次调用中执行。
     *
     * @param fetches 一系列获取请求。
     * @return 一系列获取响应。
     */
    public MultiFetchResponse multifetch(List<FetchRequest> fetches) throws IOException {
        long startTime, endTime;
        Map<Receive, Integer> response;

        synchronized (lock) {
            startTime = SystemTime.nanoseconds(); // 获取开始时间
            getOrMakeConnection(); // 确保已经建立连接

            try {
                sendRequest(new MultiFetchRequest(fetches)); // 发送组合请求
                response = getResponse(); // 获取响应
            } catch (IOException e) {
                logger.info("multifetch reconnect due to: " + e);
                // 重试一次
                try {
                    channel = connect(); // 重新连接
                    sendRequest(new MultiFetchRequest(fetches)); // 重新发送组合请求
                    response = getResponse(); // 重新获取响应
                } catch (IOException ioe) {
                    channel = null;
                    throw ioe;
                }
            } catch (Throwable e) {
                throw new RuntimeException("Error during multifetch operation", e);
            }

            endTime = SystemTime.nanoseconds(); // 获取结束时间
            SimpleConsumerStats.recordFetchRequest(endTime - startTime); // 记录请求时间
            Map.Entry<Receive, Integer> next = response.entrySet().iterator().next();

            SimpleConsumerStats.recordConsumptionThroughput(next.getKey().getBuffer().limit()); // 记录消费吞吐量

            // 错误代码将在MultiFetchResponse中的各个消息集合上设置
            return new MultiFetchResponse(
                    next.getKey().getBuffer(),
                    fetches.size(),
                    fetchOffsets(fetches) // 假设这个方法用于获取请求的偏移量
            );
        }
    }
    private long[] fetchOffsets(List<FetchRequest>  fetches) {
        long[] offsets = new long[fetches.size()];
        for (int i = 0; i < fetches.size(); i++) {
            offsets[i] = fetches.get(i).getOffset(); // 假设FetchRequest有一个getOffset()方法
        }
        return offsets;
    }
    // 获取给定时间之前的有效偏移量列表
    public long[] getOffsetsBefore(String topic, int partition, long time, int maxNumOffsets) throws IOException {
        synchronized (lock) {
            getOrMakeConnection();
            Map<Receive, Integer> response;
            try {
                sendRequest(new OffsetRequest(topic, partition, time, maxNumOffsets));
                response = getResponse();
            } catch (IOException e) {
                logger.info("获取偏移量失败，由于: " + e);
                channel = connect();
                sendRequest(new OffsetRequest(topic, partition, time, maxNumOffsets));
                response = getResponse();
            }
            Map.Entry<Receive, Integer> next = response.entrySet().iterator().next();

            return OffsetRequest.deserializeOffsetArray(next.getKey().getBuffer());
        }
    }

    /**
     * 发送请求到服务器。
     * @param request 请求对象
     */
    private void sendRequest(Request request) throws IOException {
        BoundedByteBufferSend send = new BoundedByteBufferSend(request);
         send.writeCompletely(channel); // 将请求完全写入通道
    }

    /**
     * 从服务器接收响应。
     * @return 包含接收到的数据和错误码的元组
     */
    private Map<Receive, Integer> getResponse() throws IOException {
        BoundedByteBufferReceive response = new BoundedByteBufferReceive();
        response.readCompletely(channel); // 从通道完全读取响应
        // 设置buffer的初始位置
        ByteBuffer buffer = response.getBuffer();
        int errorCode = buffer.getShort(); // 获取错误码
        return MapUtil.of(response,errorCode);
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