package org.queue.server;

import org.queue.api.RequestKeys;
import org.queue.consumer.Consumer;
import org.queue.consumer.ConsumerConfig;
import org.queue.consumer.ConsumerConnector;
import org.queue.consumer.QueueMessageStream;
import org.queue.log.Log;
import org.queue.log.LogManager;
import org.queue.message.ByteBufferMessageSet;
import org.queue.message.NoCompressionCodec;
import org.queue.network.SocketServerStats;
import org.queue.utils.SystemTime;
import org.queue.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EmbeddedConsumer {
    private  QueueServer queueServer;
    private ConsumerConnector consumerConnector;
    Map<String, List<QueueMessageStream>> topicMessageStreams;
    private final Logger logger = LoggerFactory.getLogger(EmbeddedConsumer.class);

    public EmbeddedConsumer(ConsumerConfig consumerConfig, QueueServer queueServer) {
        this.queueServer = queueServer;
        this.consumerConnector = Consumer.create(consumerConfig);
        this.topicMessageStreams = consumerConnector.createMessageStreams(consumerConfig.getEmbeddedConsumerTopicMap());
    }

    public void startup() {
        List<Thread> threadList = new ArrayList<>();
        // 假设topicMessageStreams已经创建
        for (Map.Entry<String, List<QueueMessageStream>> entry : topicMessageStreams.entrySet()) {
            String topic = entry.getKey();
            List<QueueMessageStream> streamList = entry.getValue();
            for (int i = 0; i < streamList.size(); i++) {
                int finalI = i;
                Thread thread = new Thread(new Runnable() {
                    public void run() {
                        logger.info("starting consumer thread " + finalI + " for topic " + topic);
                        LogManager logManager = queueServer.getLogManager();
                        SocketServerStats stats = queueServer.getStats();
                        try {
                            for (QueueMessageStream message : streamList) {
                                int partition = logManager.chooseRandomPartition(topic);
                                long start = SystemTime.getInstance().nanoseconds();
                                Log log = logManager.getOrCreateLog(topic, partition);
                                log.append(new ByteBufferMessageSet(new NoCompressionCodec(), message));
                                stats.recordRequest(RequestKeys.produce, SystemTime.getInstance().nanoseconds() - start);
                            }
                        } catch (Throwable e) {
                            logger.error(e + Utils.stackTrace(e));
                            logger.error(topic + " stream " + finalI + " unexpectedly exited");
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