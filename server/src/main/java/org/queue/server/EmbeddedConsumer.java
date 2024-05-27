package org.queue.server;

import org.queue.consumer.Consumer;
import org.queue.consumer.ConsumerConfig;
import org.queue.consumer.ConsumerConnector;
import org.apache.logging.log4j.LogManager;
import org.queue.network.SocketServerStats;
import org.queue.utils.SystemTime;
import org.queue.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EmbeddedConsumer {
    private  ConsumerConfig consumerConfig;
    private  QueueServer queueServer;
    private ConsumerConnector consumerConnector;
    private final Logger logger = LoggerFactory.getLogger(EmbeddedConsumer.class);

    public EmbeddedConsumer(ConsumerConfig consumerConfig, QueueServer queueServer) {
        this.consumerConfig = consumerConfig;
        this.queueServer = queueServer;
        this.consumerConnector = Consumer.create(consumerConfig);
        // 这里需要根据consumerConfig中的配置创建消息流
        // this.topicMessageStreams = consumerConnector.createMessageStreams(consumerConfig.embeddedConsumerTopicMap);
    }

    public void startup() {
        List<Thread> threadList = new ArrayList<>();
        // 假设topicMessageStreams已经创建
        Map<TopicPartition, List<ConsumerRecord>> topicMessageStreams = consumerConnector.createMessageStreams(...); // 需要根据实际情况实现
        for (Map.Entry<TopicPartition, List<ConsumerRecord>> entry : topicMessageStreams.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            List<ConsumerRecord> streamList = entry.getValue();
            for (int i = 0; i < streamList.size(); i++) {
                Thread thread = new Thread(new Runnable() {
                    public void run() {
                        logger.info("starting consumer thread " + i + " for topic " + topicPartition.topic());
                        LogManager logManager = queueServer.getLogManager();
                        SocketServerStats stats = queueServer.getStats();
                        try {
                            for (ConsumerRecord message : streamList) {
                                int partition = logManager.chooseRandomPartition(topicPartition.topic());
                                long start = SystemTime.nanoseconds();
                                Log log = logManager.getOrCreateLog(topicPartition.topic(), partition);
                                log.append(new ByteBufferMessageSet(/* 需要根据实际情况实现参数 */));
                                stats.recordRequest(RequestKeys.Produce, SystemTime.nanoseconds() - start);
                            }
                        } catch (Throwable e) {
                            logger.error(e + Utils.stackTrace(e));
                            logger.error(topicPartition.topic() + " stream " + i + " unexpectedly exited");
                        }
                    }
                });
                threadList.add(thread);
            }
        }
        for (Thread thread : threadList) {
            thread.start();
        }
    }

    public void shutdown() {
        consumerConnector.shutdown();
    }
}