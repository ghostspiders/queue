package org.queue.utils;

/**
 * 此类用于表示与特定消费者组和主题相关的ZooKeeper目录结构。
 */
public class ZKGroupTopicDirs extends ZKGroupDirs {
    private String consumerOffsetDir; // 消费者偏移量目录
    private String consumerOwnerDir;   // 消费者所有者目录

    /**
     * 构造函数，初始化消费者组和主题相关的ZooKeeper目录。
     * @param group 消费者组名称。
     * @param topic 主题名称。
     */
    public ZKGroupTopicDirs(String group, String topic) {
        super(group); // 调用父类构造函数初始化消费者组目录
        this.consumerOffsetDir = consumerGroupDir + "/offsets/" + topic;
        this.consumerOwnerDir = consumerGroupDir + "/owners/" + topic;
    }

    /**
     * 获取消费者偏移量目录的路径。
     * @return 返回消费者偏移量目录的路径。
     */
    public String getConsumerOffsetDir() {
        return consumerOffsetDir;
    }

    /**
     * 获取消费者所有者目录的路径。
     * @return 返回消费者所有者目录的路径。
     */
    public String getConsumerOwnerDir() {
        return consumerOwnerDir;
    }
}