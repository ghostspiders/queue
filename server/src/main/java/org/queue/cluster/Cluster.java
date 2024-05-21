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
import java.util.stream.Collectors;

public class Cluster {
    private Map<Integer, Broker> brokers = new HashMap<>();


    public Cluster(Iterable<Broker> brokerList) {
        for (Broker broker : brokerList) {
            brokers.put(broker.getId(), broker);
        }
    }


    public Broker getBroker(int id) {
        return brokers.get(id); // In Java, get() can return null, so you may want to handle this case
    }


    public void add(Broker broker) {
        brokers.put(broker.getId(), broker);
    }


    public boolean remove(int id) {
        return brokers.remove(id) != null;
    }


    public int size() {
        return brokers.size();
    }

    @Override
    public String toString() {
        String  brokersStr = brokers.values().stream()
                .map(r -> r.toString())
                .collect(Collectors.joining(","));

        return "Cluster(" + String.join(", ", brokersStr) + ")";
    }
}
