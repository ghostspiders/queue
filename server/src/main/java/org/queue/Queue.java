package org.queue;

import org.queue.consumer.ConsumerConfig;
import org.queue.server.QueueConfig;
import org.queue.server.QueueServerStartable;
import org.queue.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Queue {
    private static final Logger logger = LoggerFactory.getLogger(Queue.class);

    public static void main(String[] args) {
        // Utils.swallow(Level.WARN, () -> Utils.registerMBean(LoggerFactory.getLogger(""), queueLog4jMBeanName));

        boolean embeddedConsumer = false;
        String serverPath = System.getProperty("server.config");
        if (serverPath == null || serverPath.isBlank()) {
            // 如果系统属性中没有配置server.config，则使用默认的配置文件路径
            serverPath = Queue.class.getResource("/server.properties").getPath();
        }
        String consumerPath = System.getProperty("consumer.config");
        if (consumerPath == null || consumerPath.isBlank()) {
            // 如果系统属性中没有配置consumer.config，则使用默认的配置文件路径
            consumerPath = Queue.class.getResource("/consumer.properties").getPath();
        }

        try {
            QueueServerStartable queueServerStartable = null;
            Properties props = Utils.loadProps(serverPath);
            QueueConfig serverConfig = new QueueConfig(props);
            if (embeddedConsumer) {
                ConsumerConfig consumerConfig = new ConsumerConfig(Utils.loadProps(consumerPath));
                queueServerStartable = new QueueServerStartable(serverConfig, consumerConfig);
            } else {
                queueServerStartable = new QueueServerStartable(serverConfig);
            }

            // 添加关闭钩子，以便捕获Ctrl-C等中断信号
            QueueServerStartable finalQueueServerStartable = queueServerStartable;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                finalQueueServerStartable.shutdown();
                finalQueueServerStartable.awaitShutdown();
            }));

            queueServerStartable.startup();
            queueServerStartable.awaitShutdown();
        } catch (Throwable e) {
            logger.error("Error occurred while starting queue server", e);
        }
        System.exit(0);
    }
}