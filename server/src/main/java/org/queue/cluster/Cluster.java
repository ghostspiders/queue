package org.queue.cluster;

/**
 * @author gaoyvfeng
 * @ClassName Cluster
 * @description:
 * @datetime 2024年 05月 21日 17:45
 * @version: 1.0
 */
import java.util.HashMap;
import java.util.Map;

public class Cluster {

    private Map<Integer, Broker> brokers = new HashMap<>();

    // 构造函数，接受一个Broker的Iterable集合
    public Cluster(Iterable<Broker> brokerList) {
        for (Broker broker : brokerList) {
            brokers.put(broker.getId(), broker);
        }
    }

    // 默认构造函数
    public Cluster() {

    }

    // 获取指定ID的Broker
    public Broker getBroker(int id) {
        return brokers.get(id); // Java中get方法返回null，而不是抛出异常
    }

    // 添加Broker
    public void add(Broker broker) {
        brokers.put(broker.getId(), broker);
    }

    // 移除指定ID的Broker
    public void remove(int id) {
        brokers.remove(id);
    }

    // 获取集群大小
    public int size() {
        return brokers.size();
    }

    // 重写toString方法
    @Override
    public String toString() {
        return "Cluster{" + brokers.values().toString() + "}";
    }
}
