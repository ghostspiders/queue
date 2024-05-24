package org.queue.producer.async;

/**
 * @author gaoyvfeng
 * @ClassName ProducerSendThread
 * @description:
 * @datetime 2024年 05月 24日 15:32
 * @version: 1.0
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.List;

public class ProducerSendThread<T> extends Thread {
    // 日志记录器
    private static final Logger logger = LoggerFactory.getLogger(ProducerSendThread.class);

    // 线程名称
    private final String threadName;
    // 阻塞队列，用于存放待发送的消息项
    private final BlockingQueue<QueueItem<T>> queue;
    // 编码器，用于序列化消息数据
    private final Encoder<T> serializer;
    // 底层同步生产者，用于实际发送消息
    private final SyncProducer underlyingProducer;
    // 事件处理器，用于处理批处理的数据
    private final EventHandler<T> handler;
    // 回调处理器，用于在发送前后执行自定义逻辑
    private final CallbackHandler<T> cbkHandler;
    // 队列超时时间，超过此时间未发送则视为超时
    private final long queueTime;
    // 批处理大小，达到此数量的消息将被打包发送
    private final int batchSize;
    // 关闭命令，用于触发线程关闭
    private final Object shutdownCommand;
    // 关闭锁，用于控制线程关闭流程
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    // 构造函数，初始化线程所需的各种资源和参数
    public ProducerSendThread(String threadName, BlockingQueue<QueueItem<T>> queue, Encoder<T> serializer,
                              SyncProducer underlyingProducer, EventHandler<T> handler,
                              CallbackHandler<T> cbkHandler, long queueTime, int batchSize, Object shutdownCommand) {
        super(threadName);
        this.threadName = threadName;
        this.queue = queue;
        this.serializer = serializer;
        this.underlyingProducer = underlyingProducer;
        this.handler = handler;
        this.cbkHandler = cbkHandler;
        this.queueTime = queueTime;
        this.batchSize = batchSize;
        this.shutdownCommand = shutdownCommand;
    }

    // 线程执行的方法，用于处理消息发送
    @Override
    public void run() {
        try {
            // 处理所有事件
            List<QueueItem<T>> remainingEvents = processEvents();
            // 日志记录剩余事件数量
            if (logger.isDebugEnabled()) {
                logger.debug("Remaining events = " + remainingEvents.size());
            }

            // 如果有剩余事件，处理这些事件
            if (!remainingEvents.isEmpty()) {
                logger.debug("Dispatching last batch of {} events to the event handler", remainingEvents.size());
                tryToHandle(remainingEvents);
            }
        } catch (Exception e) {
            // 发送事件时发生错误
            logger.error("Error in sending events: ", e);
        } finally {
            // 计数器减一，表示线程即将关闭
            shutdownLatch.countDown();
        }
    }

    // 等待线程关闭
    public boolean awaitShutdown() throws InterruptedException {
        return shutdownLatch.await();
    }

    // 关闭线程，释放资源
    public void shutdown() {
        handler.close();
        logger.info("Shutdown thread complete");
    }

    // 处理事件的方法，从队列中取出事件并处理
    private List<QueueItem<T>> processEvents() {
        // 初始化变量
        long lastSend = System.currentTimeMillis();
        List<QueueItem<T>> events = new ArrayList<>();
        boolean full = false;

        // 持续从队列中取出事件，直到接收到关闭命令
        while (true) {
            try {
                QueueItem<T> currentQueueItem = queue.poll((int) (queueTime - (lastSend - System.currentTimeMillis())), TimeUnit.MILLISECONDS);
                if (currentQueueItem == null || currentQueueItem.getData() == shutdownCommand) {
                    break;
                }

                // 添加回调处理
                if (cbkHandler != null) {
                    events.add(cbkHandler.afterDequeuingExistingData(currentQueueItem));
                } else {
                    events.add(currentQueueItem);
                }

                // 检查是否达到批处理大小或超时时间
                long elapsed = System.currentTimeMillis() - lastSend;
                boolean expired = currentQueueItem == null;
                full = events.size() >= batchSize;

                if (full || expired) {
                    if (logger.isDebugEnabled()) {
                        if (expired) logger.debug(elapsed + " ms elapsed. Queue time reached. Sending..");
                        if (full) logger.debug("Batch full. Sending..");
                    }
                    // 处理事件
                    tryToHandle(events);
                    // 重置发送时间记录和事件列表
                    lastSend = System.currentTimeMillis();
                    events = new ArrayList<>();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while processing events", e);
            }
        }

        // 如果存在回调处理器，调用其最后一批事件处理方法
        if (cbkHandler != null) {
            logger.info("Invoking the callback handler before handling the last batch of {} events", events.size());
            List<QueueItem<T>> addedEvents = cbkHandler.lastBatchBeforeClose();
            logEvents("last batch before close", addedEvents);
            events.addAll(addedEvents);
        }

        return events;
    }

    // 尝试处理事件列表
    private void tryToHandle(List<QueueItem<T>> events) {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Handling " + events.size() + " events");
            }
            // 通过事件处理器处理事件
            handler.handle(events, underlyingProducer, serializer);
        } catch (Exception e) {
            // 处理事件时发生错误
            logger.error("Error in handling batch of " + events.size() + " events", e);
        }
    }

    // 记录事件信息
    private void logEvents(String tag, Iterable<QueueItem<T>> events) {
        if (logger.isTraceEnabled()) {
            logger.trace("events for " + tag + ":");
            for (QueueItem<T> event : events) {
                logger.trace(event.getData().toString());
            }
        }
    }
}