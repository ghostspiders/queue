package org.queue.server;

/**
 * @author gaoyvfeng
 * @ClassName QueueServerStartable
 * @description:
 * @datetime 2024年 05月 23日 17:56
 * @version: 1.0
 */
import org.queue.log.LogManager;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class QueueServerStartable {
    private KafkaServer server;
    private EmbeddedConsumer embeddedConsumer;
    private final KafkaConfig serverConfig;
    private final ConsumerConfig consumerConfig;

    public KafkaServerStartable(KafkaConfig serverConfig, ConsumerConfig consumerConfig) {
        this.serverConfig = serverConfig;
        this.consumerConfig = consumerConfig;
        init();
    }

    public KafkaServerStartable(KafkaConfig serverConfig) {
        this(serverConfig, null);
    }

    private void init() {
        server = new KafkaServer(serverConfig);
        if (consumerConfig != null) {
            embeddedConsumer = new EmbeddedConsumer(consumerConfig, server);
        }
    }

    public void startup() {
        server.startup();
        if (embeddedConsumer != null) {
            embeddedConsumer.startup();
        }
    }

    public void shutdown() {
        if (embeddedConsumer != null) {
            embeddedConsumer.shutdown();
        }
        server.shutdown();
    }

    public void awaitShutdown() {
        server.awaitShutdown();
    }
}

class EmbeddedConsumer {
    private final ConsumerConfig consumerConfig;
    private final KafkaServer kafkaServer;
    private ConsumerConnector consumerConnector;
    private LogManager logger;

    public EmbeddedConsumer(ConsumerConfig consumerConfig, KafkaServer kafkaServer) {
        this.consumerConfig = consumerConfig;
        this.kafkaServer = kafkaServer;
        this.logger = LogManager.getLogger(this.getClass());
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