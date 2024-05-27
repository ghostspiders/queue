package org.queue.utils;

/**
 * @author gaoyvfeng
 * @ClassName ZkUtils
 * @description:
 * @datetime 2024年 05月 27日 11:09
 * @version: 1.0
 */
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.queue.cluster.Broker;
import org.queue.cluster.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ZkUtils {

    public static final String ConsumersPath = "/consumers";
    public static final String BrokerIdsPath = "/brokers/ids";
    public static final String BrokerTopicsPath = "/brokers/topics";
    private static final Logger logger = LoggerFactory.getLogger(ZkUtils.class);

    /**
     * 确保ZK中存在一个持久化路径。如果不存在，则创建该路径。
     */
    public static void makeSurePersistentPathExists(ZkClient client, String path) {
        if (!client.exists(path)) {
            client.createPersistent(path, true); // 不会抛出NoNodeException或NodeExistsException
        }
    }

    // 创建父路径的私有方法
    private static void createParentPath(ZkClient client, String path) {
        String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (parentDir.length() != 0) {
            client.createPersistent(parentDir, true);
        }
    }

    // 创建临时节点的私有方法
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
     * 创建一个临时节点，并期望在节点已存在时抛出NodeExistException。
     */
    public static void createEphemeralPathExpectConflict(ZkClient client, String path, String data) {
        try {
            createEphemeralPath(client, path, data);
        } catch (ZkNodeExistsException e) {
            // 当连接丢失时可能会发生此异常；确保数据是我们打算写入的数据
            String storedData = null;
            try {
                storedData = readData(client, path);
            } catch (ZkNoNodeException e1) {
                // 节点消失了；当作节点存在处理，并让调用者处理这种情况
            } catch (Throwable e2) {
                throw e2;
            }
            if (storedData == null || !storedData.equals(data)) {
                logger.info("conflict in " + path + " data: " + data + " stored data: " + storedData);
                throw e;
            } else {
                // 否则，创建成功，正常返回
                logger.info(path + " exists with value " + data + " during connection loss; this is ok");
            }
        } catch (Throwable e2) {
            e2.printStackTrace();
        }
    }

    // 更新持久节点的值的方法
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

    // 更新临时节点的值的方法
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

    // 删除路径的方法
    public static void deletePath(ZkClient client, String path) {
        try {
            client.delete(path);
        } catch (ZkNoNodeException e) {
            // 在连接丢失事件期间可能会发生此异常，正常返回
            logger.info(path + " deleted during connection loss; this is ok");
        } catch (Throwable e2) {
            throw e2;
        }
    }

    // 递归删除路径的方法
    public static void deletePathRecursive(ZkClient client, String path) {
        try {
            client.deleteRecursive(path);
        } catch (ZkNoNodeException e) {
            // 在连接丢失事件期间可能会发生此异常，正常返回
            logger.info(path + " deleted during connection loss; this is ok");
        } catch (Throwable e2) {
            throw e2;
        }
    }

    // 读取数据的方法
    public static String readData(ZkClient client, String path) {
        return client.readData(path);
    }

    // 可能返回null的读取数据的方法
    public static String readDataMaybeNull(ZkClient client, String path) {
        return client.readData(path, true);
    }

    // 获取子节点的方法
    public static List<String> getChildren(ZkClient client, String path) {
        List<String> children = client.getChildren(path);
        return new ArrayList<>(children); // 将Java列表转换为Scala列表
    }

    // 获取子节点，父节点可能不存在的方法
    public static List<String> getChildrenParentMayNotExist(ZkClient client, String path) {
        List<String> children = null;
        try {
            children = client.getChildren(path);
        } catch (ZkNoNodeException e) {
            return Collections.emptyList();
        } catch (Throwable e2) {
            throw e2;
        }
        if (children == null) {
            // 处理空列表的情况
        }
        return new ArrayList<>(children); // 将Java列表转换为Scala列表
    }

    // 检查给定路径是否存在的方法
    public static boolean pathExists(ZkClient client, String path) {
        return client.exists(path);
    }

    // 获取路径的最后一部分的方法
    public static String getLastPart(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    // 获取集群信息的方法
    public static Cluster getCluster(ZkClient zkClient) {
        Cluster cluster = new Cluster();
        List<String> nodes = getChildrenParentMayNotExist(zkClient, BrokerIdsPath);
        for (String node : nodes) {
            String brokerZKString = readData(zkClient, BrokerIdsPath + "/" + node);
            cluster.add(Broker.createBroker(Integer.parseInt(node), brokerZKString));
        }
        return cluster;
    }

    // 获取主题的分区信息的方法
    public static Map<String, List<String>> getPartitionsForTopics(ZkClient zkClient, Iterator<String> topics) {
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
            Collections.sort(partList); // 对分区列表进行排序
            ret.put(topic, partList);
        }
        return ret;
    }

    // 设置分区的方法
    public static void setupPartition(ZkClient zkClient, int brokerId, String host, int port, String topic, int nParts) {
        String brokerIdPath = BrokerIdsPath + "/" + brokerId;
        Broker broker = new Broker(brokerId, String.valueOf(brokerId), host, port);
        createEphemeralPathExpectConflict(zkClient, brokerIdPath, broker.getZKString());
        String brokerPartTopicPath = BrokerTopicsPath + "/" + topic + "/" + brokerId;
        createEphemeralPathExpectConflict(zkClient, brokerPartTopicPath, String.valueOf(nParts));
    }

    /**
     * 删除指定代理和主题的分区信息。
     *
     * @param zkClient ZooKeeper客户端
     * @param brokerId 代理ID
     * @param topic    主题名称
     */
    public static void deletePartition(ZkClient zkClient, int brokerId, String topic) {
        String brokerIdPath = BrokerIdsPath + "/" + brokerId;
        try {
            zkClient.delete(brokerIdPath);
        } catch (Exception e) {
            logger.error("Error occurred while deleting broker ID path: " + brokerIdPath, e);
        }

        String brokerPartTopicPath = BrokerTopicsPath + "/" + topic + "/" + brokerId;
        try {
            zkClient.delete(brokerPartTopicPath);
        } catch (Exception e) {
            logger.error("Error occurred while deleting broker part topic path: " + brokerPartTopicPath, e);
        }
    }
}