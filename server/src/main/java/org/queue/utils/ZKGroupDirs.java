package org.queue.utils;

/**
 * 此类用于表示ZooKeeper中的消费者组目录结构。
 */
public class ZKGroupDirs {
    private final String group; // 消费者组名称
    private final String consumerDir; // 消费者基础目录
    private final String consumerGroupDir; // 消费者组目录
    private final String consumerRegistryDir; // 消费者注册目录

    /**
     * 构造函数，初始化消费者组相关的ZooKeeper目录。
     * @param group 消费者组名称。
     */
    public ZKGroupDirs(String group) {
        this.group = group;
        this.consumerDir = ZkUtils.ConsumersPath; // 基础消费者目录路径
        this.consumerGroupDir = consumerDir + "/" + group; // 消费者组目录路径
        this.consumerRegistryDir = consumerGroupDir + "/ids"; // 消费者注册目录路径
    }

    /**
     * 获取消费者基础目录的路径。
     * @return 返回消费者基础目录的路径。
     */
    public String getConsumerDir() {
        return consumerDir;
    }

    /**
     * 获取消费者组目录的路径。
     * @return 返回消费者组目录的路径。
     */
    public String getConsumerGroupDir() {
        return consumerGroupDir;
    }

    /**
     * 获取消费者注册目录的路径。
     * @return 返回消费者注册目录的路径。
     */
    public String getConsumerRegistryDir() {
        return consumerRegistryDir;
    }
}