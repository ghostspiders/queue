package org.queue.akka;
import akka.actor.AbstractActor;
import org.queue.server.QueueZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkActor extends AbstractActor {

    // 日志记录器
    private static final Logger logger = LoggerFactory.getLogger(ZkActor.class);
    private QueueZooKeeper queueZooKeeper;


    public ZkActor(QueueZooKeeper queueZooKeeper) {
        this.queueZooKeeper = queueZooKeeper;
    }
    //ZooKeeper注册方法的模拟
    private void registerTopicInZk(String msg) {
        queueZooKeeper.registerTopicInZk(msg);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TopicMsg.class, this::handleTopicMsg)
                .match(StopActor.class, message -> {
                    logger.info("zkActor stopped");
                    context().stop(self());
                })
                .build();
    }

    private void handleTopicMsg(TopicMsg msg) {
        try {
            registerTopicInZk(msg.getMsg());
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }

    // 定义消息类
    public static class TopicMsg {
        private final String msg;

        public TopicMsg(String msg) {
            this.msg = msg;
        }

        public String getMsg() {
            return msg;
        }
    }

    // 定义停止Actor的消息类
    public enum StopActor {
        INSTANCE;
    }
}