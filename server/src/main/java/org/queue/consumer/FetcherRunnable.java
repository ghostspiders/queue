package org.queue.consumer;

import org.I0Itec.zkclient.ZkClient;
import org.queue.api.FetchRequest;
import org.queue.api.MultiFetchResponse;
import org.queue.api.OffsetRequest;
import org.queue.cluster.Broker;
import org.queue.cluster.Partition;
import org.queue.common.ErrorMapping;
import org.queue.message.ByteBufferMessageSet;
import org.queue.utils.ZKGroupTopicDirs;
import org.queue.utils.ZkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.io.IOException;

public class FetcherRunnable extends Thread {
    private final String name; // 线程名称
    private final ZkClient zkClient; // Zookeeper客户端
    private final ConsumerConfig config; // 消费者配置
    private final Broker broker; // broker
    private final List<PartitionTopicInfo> partitionTopicInfos; // 分区主题信息列表
    private final SimpleConsumer simpleConsumer; // 简单消费者客户端
    private final Logger logger = LoggerFactory.getLogger(FetcherRunnable.class.getName()); // 日志记录器
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
        logger.info("awaiting shutdown on fetcher " + name); // 日志记录
        try {
            shutdownLatch.await(); // 等待关闭完成
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 捕获中断异常并恢复中断状态
        }
        logger.info("shutdown of fetcher " + name + " thread complete"); // 日志记录
    }

    @Override
    public void run() {
        for (PartitionTopicInfo info : partitionTopicInfos) {
            logger.info(name + " start fetching topic: " + info.getTopic() + " part: " + info.getPartition().getPartId() + " offset: "
                    + info.getFetchedOffset() + " from " + broker.getHost() + ":" + broker.getPort());
        }

        try {
            while (!stopped) {
                List<FetchRequest> fetches = new ArrayList();
                for (PartitionTopicInfo info : partitionTopicInfos) {
                    fetches.add(new FetchRequest(info.getTopic(), info.getPartition().getPartId(), info.getFetchOffset(), config.getFetchSize()));
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("fetch request: " + fetches);
                }

                MultiFetchResponse response = simpleConsumer.multifetch(fetches);
               Iterator<ByteBufferMessageSet> iterator = response.iterator();
                long read = 0L;
               while (iterator.hasNext()){
                   ByteBufferMessageSet messages = iterator.next();
                   PartitionTopicInfo info = partitionTopicInfos.iterator().next();
                   try {
                       boolean done = false;
                       if (messages.getErrorCode() == ErrorMapping.OffsetOutOfRangeCode) {
                           logger.info("offset " + info.getFetchOffset() + " out of range");
                           // see if we can fix this error
                           long resetOffset = resetConsumerOffsets(info.getTopic(), info.getPartition());
                           if (resetOffset >= 0) {
                               info.resetFetchOffset(resetOffset);
                               info.resetConsumeOffset(resetOffset);
                               done = true;
                           }
                       }
                       if (!done) {
                           read += info.enqueue(messages, info.getFetchOffset());
                       }
                   } catch (IOException e1) {
                       // something is wrong with the socket, re-throw the exception to stop the fetcher
                       throw e1;
                   } catch (Throwable e2) {
                       if (!stopped) {
                           // this is likely a repeatable error, log it and trigger an exception in the consumer
                           logger.error("error in FetcherRunnable for " + info, e2);
                           info.enqueueError(e2, info.getFetchOffset());
                       }
                       // re-throw the exception to stop the fetcher
                       throw e2;
                   }
               }


                if (logger.isDebugEnabled()) {
                    logger.debug("fetched bytes: " + read);
                }
                if (read == 0) {
                    logger.debug("backing off " + config.getBackoffIncrementMs() + " ms");
                    try {
                        Thread.sleep(config.getBackoffIncrementMs());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        } catch (Throwable e) {
            if (stopped) {
                logger.info("FetcherRunnable " + this + " interrupted");
            } else {
                logger.error("error in FetcherRunnable ", e);
            }
        }

        logger.info("stopping fetcher " + name + " to host " + broker.getHost());
        simpleConsumer.close();
        shutdownComplete();
    }

    // 标记关闭完成的方法
    private void shutdownComplete() {
        shutdownLatch.countDown();
    }

    // 重置消费者偏移量的方法
    private long resetConsumerOffsets(String topic, Partition partition) throws IOException {
        long offset = 0;
        String autoOffsetReset = config.getAutoOffsetReset(); // 获取自动偏移量重置配置

        if (OffsetRequest.SmallestTimeString.equals(autoOffsetReset)) {
            offset = OffsetRequest.EarliestTime; // 设置为最早的偏移量
        } else if (OffsetRequest.LargestTimeString.equals(autoOffsetReset)) {
            offset = OffsetRequest.LatestTime; // 设置为最晚的偏移量
        } else {
            return -1; // 如果不是预期的值，返回-1表示错误
        }

        // 从代理获取指定偏移量的时间戳
        long[] offsets = simpleConsumer.getOffsetsBefore(topic, partition.getPartId(), offset, 1);

        // 创建Zookeeper路径工具类
        ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(config.getGroupId(), topic);

        // 日志记录更新偏移量的操作
        logger.info("updating partition " + partition.getName() + " with " +
                (offset == OffsetRequest.EarliestTime ? "earliest" : "latest") +
                " offset " + offsets[0]);

        // 在Zookeeper中手动重置偏移量
        ZkUtils.updatePersistentPath(
                zkClient,
                topicDirs.getConsumerOffsetDir() + "/" + partition.getName(),
                String.valueOf(offsets[0])
        );

        return offsets[0]; // 返回获取到的偏移量
    }
}