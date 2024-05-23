package org.queue.server;

import org.queue.consumer.Consumer;
import org.queue.consumer.ConsumerConfig;
import org.queue.consumer.ConsumerConnector;
import org.apache.logging.log4j.LogManager;
import org.queue.consumer.ConsumerIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EmbeddedConsumer {
    private  ConsumerConfig consumerConfig;
    private  QueueServer kafkaServer;
    private ConsumerConnector consumerConnector;
    private final Logger logger = LoggerFactory.getLogger(EmbeddedConsumer.class);

    public EmbeddedConsumer(ConsumerConfig consumerConfig, QueueServer kafkaServer) {
        this.consumerConfig = consumerConfig;
        this.kafkaServer = kafkaServer;
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
                        LogManager logManager = kafkaServer.getLogManager();
                        SocketServerStats stats = kafkaServer.getStats();
                        try {
                            for (ConsumerRecord message : streamList) {
                                int partition = logManager.chooseRandomPartition(topicPartition.topic());
                                long start = SystemTime.nanoseconds();
                                Log log = logManager.getOrCreateLog(topicPartition.topic(), partition);
                                log.append(new ByteBufferMessageSet(/* 需要根据实际情况实现参数 */));
                                stats.recordRequest(RequestKeys.Produce, SystemTime.nanoseconds() - start);
                            }
                        } catch (Throwable e) {
                            logger.fatal(e + Utils.stackTrace(e));
                            logger.fatal(topicPartition.topic() + " stream " + i + " unexpectedly exited");
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