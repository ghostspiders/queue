package org.queue.consumer;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.zookeeper.Watcher;
import org.queue.utils.ZKGroupDirs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKSessionExpireListenner implements IZkStateListener {
    private final ZKGroupDirs dirs;
    private final String consumerIdString;
    private final TopicCount topicCount;
    private final ZKRebalancerListener loadBalancerListener;
    private static final Logger logger = LoggerFactory.getLogger(ZKSessionExpireListenner.class.getName());

    public ZKSessionExpireListenner(ZKGroupDirs dirs, String consumerIdString, TopicCount topicCount, ZKRebalancerListener loadBalancerListener) {
        this.dirs = dirs;
        this.consumerIdString = consumerIdString;
        this.topicCount = topicCount;
        this.loadBalancerListener = loadBalancerListener;
    }

    /**
     * Called when the zookeeper connection state has changed.
     *
     * @param state The new state.
     * @throws Exception On any error.
     */
    @Override
    public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {

    }

    /**
     * 调用时机：ZooKeeper会话过期并且已创建新会话之后。
     * 需要在这里重新创建任何临时节点。
     */
    @Override
    public void handleNewSession() {
        // 当收到会话过期事件时，我们失去了所有的临时节点，zkclient已经为我们重新建立了连接。
        // 我们需要在这里释放当前消费者的分区所有权，重新在消费者注册表中注册这个消费者，并触发再平衡。
        logger.info("ZK expired; release old broker partition ownership; re-register consumer " + consumerIdString);
        loadBalancerListener.resetState();
        registerConsumerInZK(dirs, consumerIdString, topicCount);
        // 显式触发该消费者的负载平衡
        loadBalancerListener.syncedRebalance();

        // 无需重新订阅子节点和状态变更。
        // 子节点变更监听器将在再平衡时读取子节点列表的过程中设置。
    }

    @Override
    public void handleSessionEstablishmentError(Throwable throwable) {
        // 这里不执行任何操作。
    }

    private void registerConsumerInZK(ZKGroupDirs dirs, String consumerIdString, TopicCount topicCount) {
        // 在ZooKeeper中注册消费者的逻辑...
    }
}