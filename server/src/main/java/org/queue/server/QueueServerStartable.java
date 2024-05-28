package org.queue.server;

/**
 * @author gaoyvfeng
 * @ClassName QueueServerStartable
 * @description:
 * @datetime 2024年 05月 23日 17:56
 * @version: 1.0
 */
import org.queue.consumer.ConsumerConfig;
import org.queue.log.LogManager;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class QueueServerStartable {
    private QueueServer server;
    private EmbeddedConsumer embeddedConsumer;
    private  QueueConfig serverConfig;
    private  ConsumerConfig consumerConfig;

    public QueueServerStartable(QueueConfig serverConfig, ConsumerConfig consumerConfig) {
        this.serverConfig = serverConfig;
        this.consumerConfig = consumerConfig;
        init();
    }

    public QueueServerStartable(QueueConfig serverConfig) {
        this(serverConfig, null);
    }

    private void init() {
        server = new QueueServer(serverConfig);
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

    public void awaitShutdown() throws InterruptedException {
        server.awaitShutdown();
    }
}

