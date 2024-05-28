package org.queue.javaapi.consumer;
import org.queue.api.FetchRequest;
import org.queue.javaapi. MultiFetchResponse;
import org.queue.javaapi.message.ByteBufferMessageSet;
import java.util.List;

/**
 * 消息队列消息的消费者
 */
public class SimpleApiConsumer {
    private  String host;           // 队列服务器的主机名
    private  int port;              // 队列服务器的端口号
    private  int soTimeout;         // Socket超时时间（毫秒）
    private  int bufferSize;        // 缓冲区大小
    private final SimpleApiConsumer underlying;  // 底层的SimpleConsumer实例

    /**
     * 构造一个新的SimpleConsumer。
     *
     * @param host 队列服务器的主机名
     * @param port 队列服务器的端口号
     * @param soTimeout Socket超时时间（毫秒）
     * @param bufferSize 缓冲区大小
     */
    public SimpleApiConsumer(String host, int port, int soTimeout, int bufferSize) {
        this.host = host;
        this.port = port;
        this.soTimeout = soTimeout;
        this.bufferSize = bufferSize;
        this.underlying = new SimpleApiConsumer(host, port, soTimeout, bufferSize);
    }

    /**
     * 从指定主题获取一组消息。
     *
     * @param request 包含主题名称、主题分区、起始字节偏移量和最大获取字节数的请求
     * @return 一组获取到的消息
     */
    public ByteBufferMessageSet fetch(FetchRequest request) {
        return underlying.fetch(request);
    }

    /**
     * 在一个调用中组合多个获取请求。
     *
     * @param fetches 一个获取请求的列表
     * @return 一个获取响应的列表
     */
    public List<MultiFetchResponse> multifetch(List<FetchRequest> fetches) {
        // 假设底层的multifetch方法能够处理List<FetchRequest>
        return underlying.multifetch(fetches);
    }

    /**
     * 获取给定时间之前（直到maxSize）的有效偏移量列表。
     * 结果是按降序排列的偏移量列表。
     *
     * @param time 时间毫秒数（-1表示获取最新可用的偏移量，-2表示获取最小可用的偏移量）
     * @return 偏移量数组
     */
    public long[] getOffsetsBefore(String topic, int partition, long time, int maxNumOffsets) {
        return underlying.getOffsetsBefore(topic, partition, time, maxNumOffsets);
    }

    /**
     * 关闭底层的SimpleConsumer并释放资源。
     */
    public void close() {
        underlying.close();
    }
}
