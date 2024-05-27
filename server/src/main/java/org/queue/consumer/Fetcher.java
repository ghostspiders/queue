package org.queue.consumer;

import java.util.*;
import java.util.logging.Logger;

public class Fetcher {
    private final ConsumerConfig config; //
    private final ZkClient zkClient; // Zookeeper客户端
    private static final FetcherRunnable[] EMPTY_FETCHER_THREADS = new FetcherRunnable[0];
    private volatile FetcherRunnable[] fetcherThreads = EMPTY_FETCHER_THREADS; // 获取线程数组
    private final Logger logger = Logger.getLogger(Fetcher.class.getName()); // 日志记录器

    /**
     * 构造函数
     */
    public Fetcher(ConsumerConfig config, ZkClient zkClient) {
        this.config = config;
        this.zkClient = zkClient;
    }

    /**
     * 关闭所有获取线程
     */
    public void shutdown() {
        // 关闭旧的获取线程，如果有的话
        for (FetcherRunnable fetcherThread : fetcherThreads) {
            fetcherThread.shutdown();
        }
        fetcherThreads = EMPTY_FETCHER_THREADS;
    }

    /**
     * 打开连接。
     */
    public void initConnections(Iterable<PartitionTopicInfo> topicInfos, Cluster cluster,
                                Iterable<BlockingQueue<FetchedDataChunk>> queuesTobeCleared) {
        shutdown;

        if (topicInfos == null) {
            return;
        }

        // 清空队列
        for (BlockingQueue<FetchedDataChunk> queue : queuesTobeCleared) {
            queue.clear();
        }

        // 按代理ID重新排序
        Map<Integer, List<PartitionTopicInfo>> m = new HashMap<>();
        for (PartitionTopicInfo info : topicInfos) {
            List<PartitionTopicInfo> lst = m.get(info.getBrokerId());
            if (lst == null) {
                m.put(info.getBrokerId(), Collections.singletonList(info));
            } else {
                lst.add(info);
                m.put(info.getBrokerId(), lst);
            }
        }

        // 为每个代理打开一个新的获取线程
        Set<Integer> ids = new HashSet<>();
        for (PartitionTopicInfo info : topicInfos) {
            ids.add(info.getBrokerId());
        }
        List<Broker> brokers = new ArrayList<>();
        for (Integer brokerId : ids) {
            brokers.add(cluster.getBroker(brokerId));
        }
        fetcherThreads = new FetcherRunnable[brokers.size()];
        int i = 0;
        for (Broker broker : brokers) {
            FetcherRunnable fetcherThread = new FetcherRunnable("FetchRunnable-" + i, zkClient, config, broker,
                    m.get(broker.getId()));
            fetcherThreads[i] = fetcherThread;
            fetcherThread.start();
            i++;
        }
    }
}