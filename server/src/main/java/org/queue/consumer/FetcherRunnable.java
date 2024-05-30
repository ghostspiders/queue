package org.queue.consumer;

import org.I0Itec.zkclient.ZkClient;
import org.queue.api.OffsetRequest;
import org.queue.cluster.Broker;
import org.queue.cluster.Partition;
import org.queue.utils.ZKGroupTopicDirs;
import org.queue.utils.ZkUtils;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;
import java.io.IOException;

public class FetcherRunnable extends Thread {
    private final String name; // 线程名称
    private final ZkClient zkClient; // Zookeeper客户端
    private final ConsumerConfig config; // 消费者配置
    private final Broker broker; // broker
    private final List<PartitionTopicInfo> partitionTopicInfos; // 分区主题信息列表
    private final SimpleConsumer simpleConsumer; // 简单消费者客户端
    private final Logger logger = Logger.getLogger(FetcherRunnable.class.getName()); // 日志记录器
    private final CountDownLatch shutdownLatch = new CountDownLatch(1); // 关闭操作的计数器
    private volatile boolean stopped = false; // 停止标志

    // 构造函数
    public FetcherRunnable(String name, ZkClient zkClient, ConsumerConfig config, Broker broker,
                           List<PartitionTopicInfo> partitionTopicInfos) {
        super(name); // 调用Thread类的构造函数设置线程名称
        this.name = name;
        this.zkClient = zkClient;
        this.config = config;
        this.broker = broker;
        this.partitionTopicInfos = partitionTopicInfos;
        // 初始化SimpleConsumer
        this.simpleConsumer = new SimpleConsumer(
                broker.getHost(), broker.getPort(), config.getSocketTimeoutMs(),
                config.getSocketBufferSize()
        );
    }

    // 关闭方法，停止线程
    public void shutdown() {
        stopped = true; // 设置停止标志
        interrupt(); // 中断线程
        logger.fine("awaiting shutdown on fetcher " + name); // 日志记录
        try {
            shutdownLatch.await(); // 等待关闭完成
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 捕获中断异常并恢复中断状态
        }
        logger.fine("shutdown of fetcher " + name + " thread complete"); // 日志记录
    }

    // 线程执行的方法
    @Override
    public void run() {
        // 日志记录开始获取信息
        for (PartitionTopicInfo info : partitionTopicInfos) {
            logger.info(name + " start fetching topic: " + info.topic() + " part: " + info.partition().partitionId() +
                    " offset: " + info.fetchOffset() + " from " + broker.host() + ":" + broker.port());
        }

        try {
            while (!stopped) {
                // 构建获取请求列表
                List<FetchRequest> fetches = new ArrayList<>();
                for (PartitionTopicInfo info : partitionTopicInfos) {
                    fetches.add(new FetchRequest(
                            info.topic(), info.partition().partitionId(), info.fetchOffset(),
                            config.fetchSize()
                    ));
                }

                // 日志记录获取请求
                if (logger.isLoggable(java.util.logging.Level.FINER)) {
                    logger.finer("fetch request: " + fetches.toString());
                }

                // 执行多获取操作
                Map<TopicAndPartition, ByteBufferMessageSet> response =
                        simpleConsumer.multiFetch(fetches);

                long read = 0L; // 读取的字节数

                // 处理获取响应
                for (Map.Entry<TopicAndPartition, ByteBufferMessageSet> entry : response.entrySet()) {
                    PartitionTopicInfo info = getInfoByTopicAndPartition(entry.getKey());
                    try {
                        boolean done = false;
                        if (entry.getValue().getErrorCode() == ErrorMapping.OffsetOutOfRangeCode()) {
                            logger.info("offset " + info.fetchOffset() + " out of range");
                            // 重置消费者偏移量
                            long resetOffset = resetConsumerOffsets(info.topic(), info.partition());
                            if (resetOffset >= 0) {
                                info.setFetchOffset(resetOffset);
                                info.setConsumeOffset(resetOffset);
                                done = true;
                            }
                        }
                        if (!done) {
                            read += info.enqueue(entry.getValue(), info.fetchOffset());
                        }
                    } catch (IOException e1) {
                        // 套接字出现问题，抛出异常以停止获取器
                        throw e1;
                    } catch (Throwable e2) {
                        if (!stopped) {
                            // 可重复的错误，记录日志并在消费者中触发异常
                            logger.log(java.util.logging.Level.SEVERE, "error in FetcherRunnable for " + info, e2);
                            info.enqueueError(e2, info.fetchOffset());
                        }
                        // 抛出异常以停止获取器
                        throw e2;
                    }
                }

                // 日志记录已读取的字节数
                if (logger.isLoggable(java.util.logging.Level.FINER)) {
                    logger.finer("fetched bytes: " + read);
                }
                if (read == 0) {
                    logger.fine("backing off " + config.backoffIncrementMs() + " ms");
                    try {
                        Thread.sleep(config.backoffIncrementMs());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // 捕获中断异常并恢复中断状态
                    }
                }
            }
        } catch (Throwable e) {
            if (!stopped) {
                logger.log(java.util.logging.Level.SEVERE, "error in FetcherRunnable ", e);
            }
        }
        // 日志记录停止获取器信息
        logger.info("stopping fetcher " + name + " to host " + broker.host());
        try {
            simpleConsumer.close();
        } catch (IOException e) {
            // 忽略关闭SimpleConsumer时发生的IOException
        }
        shutdownComplete(); // 标记关闭完成
    }

    // 标记关闭完成的方法
    private void shutdownComplete() {
        shutdownLatch.countDown();
    }

    // 根据TopicAndPartition获取PartitionTopicInfo
    private PartitionTopicInfo getInfoByTopicAndPartition(TopicAndPartition tap) {
        // 实现该方法的逻辑，根据TopicAndPartition获取对应的PartitionTopicInfo
        return null; // 返回找到的PartitionTopicInfo对象
    }

    // 重置消费者偏移量的方法
    private long resetConsumerOffsets(String topic, Partition partition) {
        long offset = 0;
        String autoOffsetReset = config.autoOffsetReset(); // 获取自动偏移量重置配置

        if (OffsetRequest.SmallestTimeString.equals(autoOffsetReset)) {
            offset = OffsetRequest.EARLIEST_TIME; // 设置为最早的偏移量
        } else if (OffsetRequest.LargestTimeString.equals(autoOffsetReset)) {
            offset = OffsetRequest.LATEST_TIME; // 设置为最晚的偏移量
        } else {
            return -1; // 如果不是预期的值，返回-1表示错误
        }

        // 从代理获取指定偏移量的时间戳
        long[] offsets = simpleConsumer.getOffsetsBefore(topic, partition.partId(), offset, 1);

        // 创建Zookeeper路径工具类
        ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(config.groupId(), topic);

        // 日志记录更新偏移量的操作
        logger.info("updating partition " + partition.name() + " with " +
                (offset == OffsetRequest.EARLIEST_TIME ? "earliest" : "latest") +
                " offset " + offsets[0]);

        // 在Zookeeper中手动重置偏移量
        ZkUtils.updatePersistentPath(
                zkClient,
                topicDirs.consumerOffsetDir() + "/" + partition.name(),
                String.valueOf(offsets[0])
        );

        return offsets[0]; // 返回获取到的偏移量
    }
}