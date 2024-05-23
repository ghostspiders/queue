package org.queue.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * @author gaoyvfeng
 * @ClassName Consumer
 * @description:
 * @datetime 2024年 05月 22日 17:40
 * @version: 1.0
 */
public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    private static final String consumerStatsMBeanName = "queue:type=queue.ConsumerStats";

    /**
     * 创建ConsumerConnector。
     *
     * @param config 配置信息，至少需要指定消费者的groupid和zookeeper连接字符串zk.connect。
     * @return Kafka的消费者连接器。
     */
    public static ConsumerConnector create(ConsumerConfig config) {
        ConsumerConnector consumerConnect = new ZookeeperConsumerConnector(config);
        Utils.swallow(Level.TRACE, () -> Utils.registerMBean(consumerConnect, consumerStatsMBeanName));
        return consumerConnect;
    }
}