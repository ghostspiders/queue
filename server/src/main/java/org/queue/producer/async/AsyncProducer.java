package org.queue.producer.async;

import org.queue.api.ProducerRequest;
import org.queue.producer.ProducerConfig;
import org.queue.producer.SyncProducer;
import org.queue.serializer.Encoder;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Logger;
import javax.management.ObjectName;
import javax.management.MBeanServer;
import javax.management.JMException;

public class AsyncProducer<T> {
    // 定义一些静态常量
    private static final Object Shutdown = new Object(); // 关闭信号对象
    private static final Random Random = new Random(); // 随机数生成器
    private static final String ProducerMBeanName = "org.queue.producer.Producer:type=AsyncProducerStats"; // MBean 名称
    private static final Logger logger = Logger.getLogger(AsyncProducer.class.getName()); // 日志记录器

    // 定义类的成员变量
    private final ProducerConfig config; // 异步生产者配置
    private final SyncProducer producer; // 同步生产者
    private final Encoder<T> serializer; // 序列化器
    private final EventHandler<T> eventHandler; // 事件处理器
    private final Properties eventHandlerProps; // 事件处理器属性
    private final CallbackHandler<T> cbkHandler; // 回调处理器
    private final Properties cbkHandlerProps; // 回调处理器属性
    private final AtomicBoolean closed = new AtomicBoolean(false); // 标记是否已关闭
    private final BlockingQueue<QueueItem<T>> queue; // 阻塞队列，用于存放待发送的消息
    private final ProducerSendThread sendThread; // 生产者发送线程
    private AsyncProducerStats<T> asyncProducerStats; // 异步生产者统计信息

    // 构造函数
    public AsyncProducer(ProducerConfig config, SyncProducer producer, Encoder<T> serializer,
                         EventHandler<T> eventHandler, Properties eventHandlerProps,
                         CallbackHandler<T> cbkHandler, Properties cbkHandlerProps) {
        this.config = config;
        this.producer = producer;
        this.serializer = serializer;
        this.eventHandler = eventHandler;
        this.eventHandlerProps = eventHandlerProps;
        this.cbkHandler = cbkHandler;
        this.cbkHandlerProps = cbkHandlerProps;
        this.queue = new LinkedBlockingQueue<>(config.getQueueSize()); // 初始化队列
        this.sendThread = createProducerSendThread(); // 创建发送线程
    }

    // 创建生产者发送线程
    private ProducerSendThread createProducerSendThread() {
        EventHandler<T> actualEventHandler = (eventHandler != null) ? eventHandler : new DefaultEventHandler<>(new ProducerConfig(config.getProps()), cbkHandler);
        return new ProducerSendThread("ProducerSendThread-" + Random.nextInt(), queue, serializer, producer, actualEventHandler, cbkHandler, config.getQueueTime(), config.getBatchSize(), Shutdown);
    }

    // 开始发送线程
    public void start() {
        sendThread.start();
    }

    // 发送消息到指定主题
    public void send(String topic, T event) {
        send(topic, event, ProducerRequest.RandomPartition);
    }

    // 发送消息到指定主题和分区
    public void send(String topic, T event, int partition) {
        if (closed.get()) {
            throw new IllegalStateException("Attempt to add event to a closed queue.");
        }

        QueueItem<T> data = new QueueItem<>(event, topic, partition); // 创建队列项
        if (cbkHandler != null) {
            data = cbkHandler.beforeEnqueue(data); // 回调处理前置操作
        }

        boolean added = queue.offer(data); // 尝试将消息添加到队列
        if (cbkHandler != null) {
            cbkHandler.afterEnqueue(data, added); // 回调处理后置操作
        }

        if (!added) {
            logger.severe("Event queue is full of unsent messages, could not send event: " + event.toString());
            throw new IllegalStateException("Event queue is full of unsent messages, could not send event: " + event.toString());
        }
    }

    // 关闭生产者
    public void close() {
        if (cbkHandler != null) {
            cbkHandler.close();
            logger.info("Closed the callback handler");
        }
        try {
            queue.put(new QueueItem<>(Shutdown, null, -1)); // 添加关闭信号到队列
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        sendThread.shutdown(); // 关闭发送线程
        try {
            sendThread.join(); // 等待发送线程结束
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        producer.close(); // 关闭生产者
        closed.set(true); // 设置关闭状态
        logger.info("Closed AsyncProducer");
    }

}