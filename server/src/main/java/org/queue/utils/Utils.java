package org.queue.utils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

public class ZkUtils {

    public static final String ConsumersPath = "/consumers";
    public static final String BrokerIdsPath = "/brokers/ids";
    public static final String BrokerTopicsPath = "/brokers/topics";
    private static final Logger logger = Logger.getLogger(ZkUtils.class);

    /**
     * 确保ZK中的持久路径存在。如果不存在，则创建该路径。
     */
    public static void makeSurePersistentPathExists(ZkClient client, String path) {
        if (!client.exists(path)) {
            client.createPersistent(path, true); // 不会抛出NoNodeException或NodeExistsException
        }
    }

    /**
     * 创建父路径。
     */
    private static void createParentPath(ZkClient client, String path) {
        String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (parentDir.length() != 0) {
            client.createPersistent(parentDir, true);
        }
    }

    /**
     * 使用给定的路径和数据创建一个临时节点。如果需要，创建父级路径。
     */
    private static void createEphemeralPath(ZkClient client, String path, String data) {
        try {
            client.createEphemeral(path, data);
        } catch (ZkNoNodeException e) {
            e.printStackTrace();
            createParentPath(client, path);
            client.createEphemeral(path, data);
        }
    }

    /**
     * 使用给定的路径和数据创建一个临时节点。
     * 如果节点已存在，则抛出NodeExistException。
     */
    public static void createEphemeralPathExpectConflict(ZkClient client, String path, String data) {
        try {
            createEphemeralPath(client, path, data);
        } catch (ZkNodeExistsException e) {
            // 当连接丢失时可能会发生这种情况；确保数据是我们打算写入的内容
            String storedData = null;
            try {
                storedData = readData(client, path);
            } catch (ZkNoNodeException e1) {
                // 节点消失了；当作节点已存在处理，并让调用者处理这种情况
            } catch (Throwable e2) {
                throw e2;
            }
            if (storedData == null || !storedData.equals(data)) {
                logger.info("冲突在 " + path + " 数据: " + data + " 存储的数据: " + storedData);
                throw e;
            } else {
                // 否则，创建成功，正常返回
                logger.info(path + " 在连接丢失期间存在，值为 " + data + "；这是正常的");
            }
        } catch (Throwable e2) {
            e2.printStackTrace();
        }
    }

    /**
     * 使用给定的路径和数据更新持久节点的值。
     * 如果需要，创建父目录。永远不抛出NodeExistException。
     */
    public static void updatePersistentPath(ZkClient client, String path, String data) {
        try {
            client.writeData(path, data);
        } catch (ZkNoNodeException e) {
            createParentPath(client, path);
            try {
                client.createPersistent(path, data);
            } catch (ZkNodeExistsException e2) {
                client.writeData(path, data);
            } catch (Throwable e2) {
                throw e2;
            }
        } catch (Throwable e2) {
            throw e2;
        }
    }

    /**
     * 使用给定的路径和数据更新临时节点的值。
     * 如果需要，创建父目录。永远不抛出NodeExistException。
     */
    public static void updateEphemeralPath(ZkClient client, String path, String data) {
        try {
            client.writeData(path, data);
        } catch (ZkNoNodeException e) {
            createParentPath(client, path);
            client.createEphemeral(path, data);
        } catch (Throwable e2) {
            throw e2;
        }
    }

    public static void deletePath(ZkClient client, String path) {
        try {
            client.delete(path);
        } catch (ZkNoNodeException e) {
            // 在连接丢失事件期间可能会发生这种情况，正常返回
            logger.info(path + " 在连接丢失期间被删除；这是正常的");
        } catch (Throwable e2) {
            throw e2;
        }
    }

    public static void deletePathRecursive(ZkClient client, String path) {
        try {
            client.deleteRecursive(path);
        } catch (ZkNoNodeException e) {
            // 在连接丢失事件期间可能会发生这种情况，正常返回
            logger.info(path + " 在连接丢失期间被递归删除；这是正常的");
        } catch (Throwable e2) {
            throw e2;
        }
    }

    public static String readData(ZkClient client, String path) {
        return client.readData(path);
    }

    public static String readDataMaybeNull(ZkClient client, String path) {
        return client.readData(path, true);
    }

    public static List<String> getChildren(ZkClient client, String path) {
        return client.getChildren(path);
    }

    /**
     * 获取给定路径的子节点列表。
     * 如果父路径不存在，则返回空列表。
     */
    public static List<String> getChildrenParentMayNotExist(ZkClient client, String path) {
        try {
            return client.getChildren(path);
        } catch (ZkNoNodeException e) {
            return new ArrayList<>(); // 如果节点不存在，返回空列表
        } catch (ZkException e) {
            logger.log(java.util.logging.Level.SEVERE, "Error getting children from ZooKeeper", e);
            return null;
        }
    }

    /**
     * 检查给定路径是否存在。
     */
    public static boolean pathExists(ZkClient client, String path) {
        try {
            return client.exists(path);
        } catch (ZkException e) {
            logger.log(java.util.logging.Level.SEVERE, "Error checking path existence in ZooKeeper", e);
            return false;
        }
    }

    /**
     * 获取路径的最后一部分。
     */
    public static String getLastPart(String path) {
        int lastSlashIndex = path.lastIndexOf('/');
        return (lastSlashIndex != -1) ? path.substring(lastSlashIndex + 1) : path;
    }

    // 假设Cluster和Broker类已定义在其他地方
    public static Cluster getCluster(ZkClient zkClient) throws ZkException {
        Cluster cluster = new Cluster();
        List<String> nodes = getChildrenParentMayNotExist(zkClient, BrokerIdsPath);
        for (String node : nodes) {
            String brokerZKString = readData(zkClient, BrokerIdsPath + "/" + node);
            Broker broker = Broker.createBroker(Integer.parseInt(node), brokerZKString);
            cluster.add(broker);
        }
        return cluster;
    }

    public static Map<String, List<String>> getPartitionsForTopics(ZkClient zkClient, Iterator<String> topics) throws ZkException {
        Map<String, List<String>> ret = new HashMap<>();
        while (topics.hasNext()) {
            String topic = topics.next();
            List<String> partList = new ArrayList<>();
            List<String> brokers = getChildrenParentMayNotExist(zkClient, BrokerTopicsPath + "/" + topic);
            for (String broker : brokers) {
                int nParts = Integer.parseInt(readData(zkClient, BrokerTopicsPath + "/" + topic + "/" + broker));
                for (int part = 0; part < nParts; part++) {
                    partList.add(broker + "-" + part);
                }
            }
            Collections.sort(partList);
            ret.put(topic, partList);
        }
        return ret;
    }

    public static void setupPartition(ZkClient zkClient, int brokerId, String host, int port, String topic, int nParts) throws ZkException {
        String brokerIdPath = BrokerIdsPath + "/" + brokerId;
        Broker broker = new Broker(brokerId, String.valueOf(brokerId), host, port);
        createEphemeralPathExpectConflict(zkClient, brokerIdPath, broker.getZKString());
        String brokerPartTopicPath = BrokerTopicsPath + "/" + topic + "/" + brokerId;
        createEphemeralPathExpectConflict(zkClient, brokerPartTopicPath, String.valueOf(nParts));
    }

    public static void deletePartition(ZkClient zkClient, int brokerId, String topic) throws ZkException {
        String brokerIdPath = BrokerIdsPath + "/" + brokerId;
        zkClient.delete(brokerIdPath);
        String brokerPartTopicPath = BrokerTopicsPath + "/" + topic + "/" + brokerId;
        zkClient.delete(brokerPartTopicPath);
    }

}