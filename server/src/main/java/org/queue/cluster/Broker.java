package org.queue.cluster;
import org.queue.utils.Utils;
/**
 * @author gaoyvfeng
 * @ClassName Broker
 * @description:
 * @datetime 2024年 05月 21日 17:47
 * @version: 1.0
 */
public class Broker {
    private final int id;
    private final String creatorId;
    private final String host;
    private final int port;

    public Broker(int id, String creatorId, String host, int port) {
        this.id = id;
        this.creatorId = creatorId;
        this.host = host;
        this.port = port;
    }

    public String getZKString() {
        final String zkStr = new String(creatorId + ":" + host + ":" + port);
        return zkStr;
    }

    public  Broker createBroker(int id, String brokerInfoString) {
        String[] brokerInfo = brokerInfoString.split(":");
        return new Broker(id, brokerInfo[0], brokerInfo[1], Integer.parseInt(brokerInfo[2]));
    }

    @Override
    public String toString() {
        return "id:" + id + ",creatorId:" + creatorId + ",host:" + host + ",port:" + port;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Broker broker = (Broker) obj;
        return id == broker.id && host.equals(broker.host) && port == broker.port;
    }

    @Override
    public int hashCode() {
        return Utils.hashcode(id, host, port);
    }

    public Integer getId() {
        return this.id;
    }
}
