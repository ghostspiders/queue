package org.queue;

import org.queue.consumer.ConsumerConfig;
import org.queue.server.KafkaConfig;
import org.queue.server.KafkaServerStartable;
import org.queue.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Queue {
    private static final Logger logger = LoggerFactory.getLogger(Queue.class);

    public static void main(String[] args) {
        // 注释掉的代码是关于Kafka的Log4j MBean的注册，具体用途需要结合项目情况
        // Utils.swallow(Level.WARN, () -> Utils.registerMBean(LoggerFactory.getLogger(""), kafkaLog4jMBeanName));

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
            KafkaServerStartable kafkaServerStartable = null;
            Properties props = Utils.loadProps(serverPath);
            KafkaConfig serverConfig = new KafkaConfig(props);
            if (embeddedConsumer) {
                ConsumerConfig consumerConfig = new ConsumerConfig(Utils.loadProps(consumerPath));
                kafkaServerStartable = new KafkaServerStartable(serverConfig, consumerConfig);
            } else {
                kafkaServerStartable = new KafkaServerStartable(serverConfig);
            }

            // 添加关闭钩子，以便捕获Ctrl-C等中断信号
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaServerStartable.shutdown();
                kafkaServerStartable.awaitShutdown();
            }));

            kafkaServerStartable.startup();
            kafkaServerStartable.awaitShutdown();
        } catch (Throwable e) {
            logger.error("Error occurred while starting Kafka server", e);
        }
        System.exit(0);
    }
}