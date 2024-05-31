package org.queue.server;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.Watcher;
import org.queue.cluster.Broker;
import org.queue.log.LogManager;
import org.queue.utils.StringSerializer;
import org.queue.utils.ZkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class QueueZooKeeper {
    // 日志记录器
    private static final Logger logger = LoggerFactory.getLogger(QueueZooKeeper.class.getName());
    // Queue配置对象
    private QueueConfig config;
    // 日志管理器
    private LogManager logManager;
    // ZooKeeper客户端
    private ZkClient zkClient;
    // 已注册的主题列表
    private List<String> topics;
    // 同步锁，用于线程安全
    private final Object lock = new Object();

    // 构造函数
    public QueueZooKeeper(QueueConfig config, LogManager logManager) {
        this.config = config;
        this.logManager = logManager;
        this.topics = new CopyOnWriteArrayList<>();
    }

    // 初始化方法，启动ZooKeeper客户端
    public void startup() {
        try {
            logger.info("Connecting to ZK: " + config.getZkConnect());
            // 创建ZooKeeper连接
            ZkConnection zkConnection = new ZkConnection(config.getZkConnect(), config.getZkSessionTimeoutMs());
            zkClient = new ZkClient(zkConnection, config.getZkConnectionTimeoutMs(), new StringSerializer());
            // 注册状态变化监听器
            zkClient.subscribeStateChanges(new SessionExpireListener());
        } catch (Exception e) {
            logger.error("Failed to start ZK client: " + e.getMessage());
        }
    }

    // 在ZooKeeper中注册Broker
    public void registerBrokerInZk() {
        try {
            String brokerIdPath = ZkUtils.BrokerIdsPath + "/" + config.getBrokerId();
            logger.info("Registering broker " + brokerIdPath);
            String hostName = (config.getHostName() == null) ? InetAddress.getLocalHost().getHostAddress() : config.getHostName();
            String creatorId = hostName + "-" + System.currentTimeMillis();
            // 创建Broker对象
            Broker broker = new Broker(config.getBrokerId(), creatorId, hostName, config.getPort());
            // 在ZooKeeper中创建Broker的临时节点
            ZkUtils.createEphemeralPathExpectConflict(zkClient, brokerIdPath, broker.getZKString());
            logger.info("Registering broker " + brokerIdPath + " succeeded with " + broker);
        } catch (Exception e) {
            logger.error("Failed to register broker in ZK: " + e.getMessage());
        }
    }

    // 注册主题到ZooKeeper
    public void registerTopicInZk(String topic) {
        registerTopicInZkInternal(topic);
        synchronized (lock) {
            topics.add(topic);
        }
    }

    // 注册主题到ZooKeeper的内部实现
    private void registerTopicInZkInternal(String topic) {
        String brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic + "/" + config.getBrokerId();
        int numParts = logManager.getTopicPartitionsMap().getOrDefault(topic, config.getNumPartitions());
        logger.info("Begin registering broker topic " + brokerTopicPath + " with " + numParts + " partitions");
        ZkUtils.createEphemeralPathExpectConflict(zkClient, brokerTopicPath, Integer.toString(numParts));
        logger.info("End registering broker topic " + brokerTopicPath);
    }

    // ZooKeeper会话过期监听器
    private class SessionExpireListener implements org.I0Itec.zkclient.IZkStateListener {

        @Override
        public void handleStateChanged(Watcher.Event.KeeperState keeperState){

        }

        @Override
        public void handleNewSession() {
            try {
                logger.info("Re-registering broker info in ZK for broker " + config.getBrokerId());
                registerBrokerInZk();
                synchronized (lock) {
                    logger.info("Re-registering broker topics in ZK for broker " + config.getBrokerId());
                    for (String topic : topics) {
                        registerTopicInZkInternal(topic);
                    }
                }
                logger.info("Done re-registering broker");
            } catch (Exception e) {
                logger.error("Failed to re-register broker in ZK: " + e.getMessage());
            }
        }

        @Override
        public void handleSessionEstablishmentError(Throwable error) {
            // 处理会话建立错误
        }
    }

    // 关闭ZooKeeper客户端连接
    public void close() {
        if (zkClient != null) {
            logger.info("Closing zookeeper client...");
            zkClient.close();
        }
    }
}