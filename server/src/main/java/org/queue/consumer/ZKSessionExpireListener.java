package org.queue.consumer;

/**
 * @author gaoyvfeng
 * @ClassName ZKSessionExpireListenner
 * @description:
 * @datetime 2024年 05月 24日 10:38
 * @version: 1.0
 */
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKSessionExpireListener implements IZkStateListener {
    private static final Logger logger = LoggerFactory.getLogger(ZKSessionExpireListener.class);
    private ZKGroupDirs dirs;
    private String consumerIdString;
    private TopicCount topicCount;
    private ZKRebalancerListener loadBalancerListener;

    public ZKSessionExpireListener(ZKGroupDirs dirs, String consumerIdString, TopicCount topicCount, ZKRebalancerListener loadBalancerListener) {
        this.dirs = dirs;
        this.consumerIdString = consumerIdString;
        this.topicCount = topicCount;
        this.loadBalancerListener = loadBalancerListener;
    }

    @Override
    public void handleStateChanged(KeeperState state) throws Exception {
        // 什么也不做，因为zkclient会为我们重新连接。
    }

    @Override
    public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {

    }

    /**
     * 在ZooKeeper会话过期并且已创建新会话后调用。
     * 您可能需要在这里重新创建任何临时节点。
     *
     * @throws Exception 出现任何错误时抛出。
     */
    @Override
    public void handleNewSession() throws Exception {
        /**
         * 当我们收到一个会话过期事件时，我们失去了所有的临时节点，zkclient已经为我们重新建立了连接。
         * 我们需要释放当前消费者的所有权，在消费者注册表中重新注册此消费者，并触发重新平衡。
         */
        logger.info("ZK expired; release old broker partition ownership; re-register consumer " + consumerIdString);
        loadBalancerListener.resetState(); // 重置负载均衡器的状态
        registerConsumerInZK(dirs, consumerIdString, topicCount); // 在ZK中重新注册消费者
        // 明确地为这个消费者触发负载均衡
        loadBalancerListener.syncedRebalance();

        // 无需重新订阅子节点和状态更改。
        // 子节点变化观察者将在我们读取子节点列表时，在重新平衡中设置。
    }

    @Override
    public void handleSessionEstablishmentError(Throwable throwable) {
        // 什么也不做，或者可以添加错误处理逻辑
    }
}
