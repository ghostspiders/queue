package org.queue.log;

/**
 * @author gaoyvfeng
 * @ClassName LogManager
 * @description:
 * @datetime 2024年 05月 23日 16:12
 * @version: 1.0
 */

import akka.actor.ActorSystem;
import akka.actor.Props;
import org.queue.common.InvalidPartitionException;
import org.queue.common.InvalidTopicException;
import org.queue.server.QueueConfig;
import org.queue.server.QueueZooKeeper;
import org.queue.utils.Pool;
import org.queue.utils.QueueScheduler;
import org.queue.utils.Time;
import org.queue.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * LogManager类负责创建和分发日志
 */
public class LogManager {
    private final QueueConfig config;
    private final QueueScheduler scheduler;
    private final Time time;
    private final long logCleanupIntervalMs;
    private final long logCleanupDefaultAgeMs;
    private final boolean needRecovery;
    private File logDir;
    private int numPartitions;
    private long maxSize;
    private int flushInterval;
    private Map<String, Integer> topicPartitionsMap;
    private CountDownLatch startupLatch;
    private QueueScheduler logFlusherScheduler;
    private Map<String, Integer> logFlushIntervalMap;
    private Map<String, Long> logRetentionMSMap;
    private static Logger logger = LoggerFactory.getLogger(LogManager.class);

    // 使用ConcurrentHashMap保证线程安全
    private Pool<String, Pool<Integer, Log>> logs = new Pool();
    private QueueZooKeeper queueZooKeeper;
    private ScheduledExecutorService cleanupScheduler;
    private Random random = new Random();
    public LogManager(QueueConfig config, QueueScheduler scheduler, Time time,
                      long logCleanupIntervalMs, long logCleanupDefaultAgeMs, boolean needRecovery) throws IOException {
        this.config = config;
        this.scheduler = scheduler;
        this.time = time;
        this.logCleanupIntervalMs = logCleanupIntervalMs;
        this.logCleanupDefaultAgeMs = logCleanupDefaultAgeMs;
        this.needRecovery = needRecovery;

        this.logDir = new File(config.getLogDir());
        this.numPartitions = config.getNumPartitions();
        this.maxSize = config.getLogFileSize();
        this.flushInterval = config.getFlushInterval();
        this.topicPartitionsMap = config.getTopicPartitionsMap();
        if (config.isEnableZookeeper()){
            this.startupLatch = new CountDownLatch(1);
        }else {
            this.startupLatch = null;
        }
        logFlusherScheduler = new QueueScheduler(1, "queue-logflusher-",false);


        // 初始化刷新间隔映射
        logFlushIntervalMap = config.getFlushIntervalMap();

        // 初始化日志保留时间映射
        logRetentionMSMap = getLogRetentionMSMap(config.getLogRetentionHoursMap());
            // 初始化日志目录
        initLogDir();
        scheduleCleanupTask();
        initZookeeperIfNeeded();
    }

    /**
     * 初始化日志目录
     */
    private void initLogDir() throws IOException {
        if (!logDir.exists()) {
            logger.info("未找到日志目录，正在创建 '{}'", logDir.getAbsolutePath());
            logDir.mkdirs();
        }
        if (!logDir.isDirectory() || !logDir.canRead()) {
            throw new IllegalArgumentException(logDir.getAbsolutePath() + " 不是一个可读的日志目录。");
        }

        File[] subDirs = logDir.listFiles();
        if (subDirs != null) {
            for (File dir : subDirs) {
                if (!dir.isDirectory()) {
                    logger.warn("跳过无法解释的文件 '{}'--它应该在这里吗？", dir.getAbsolutePath());
                } else {
                    logger.info("正在加载日志 '{}'", dir.getName());
                    Log log = new Log(dir, maxSize, flushInterval, needRecovery);
                    Map<String, Integer> topicPartion = Utils.getTopicPartition(dir.getName());
                    topicPartion.forEach((key, value) -> {
                        logs.putIfNotExists(key, new Pool())
                                .put(value, log);
                    });

                }
            }
        }
    }

    /**
     * 安排清理任务，用于删除旧的日志文件。
     */
    public void scheduleCleanupTask() {
        if (scheduler != null) {
            // 日志清理任务每logCleanupIntervalMs毫秒执行一次
            logger.info("Starting log cleaner every {} ms", logCleanupIntervalMs);
            // 使用Akka调度器安排周期性任务
            scheduler.scheduleWithRate(this::cleanupLogs, 60 * 1000, logCleanupIntervalMs);
        }
    }

    /**
     * 如果配置启用了Zookeeper，初始化ZooKeeper并启动。
     */
    public void initZookeeperIfNeeded() {
        if (config.isEnableZookeeper()) {
            queueZooKeeper = new QueueZooKeeper(config, this);
            queueZooKeeper.startup();

            // 创建ActorSystem
            ActorSystem actorSystem = ActorSystem.create();
            Props props = Props.create(ZkActor.class, new Creator<ZkActor>() {
                @Override
                public ZkActor create() throws Exception {
                    return new ZkActor();
                }
            });
            // 创建并启动ZkActor
            actorSystem.actorOf(props, "ZkActor");
        }
    }

    // 私有方法，将日志保留时间（小时）转换为毫秒
    private Map<String, Long> getLogRetentionMSMap(Map<String, Integer> logRetentionHourMap) {
        Map<String, Long> ret = new HashMap<>();
        for (Map.Entry<String, Integer> entry : logRetentionHourMap.entrySet()) {
            ret.put(entry.getKey(), entry.getValue().longValue() * 60L * 60L * 1000L);
        }
        return ret;
    }

    // 注册Broker到ZooKeeper
    public void startup() {
        if (config.isEnableZookeeper()) {
            // 注册Broker和主题
            queueZooKeeper.registerBrokerInZk();
            for (String topic : getAllTopics()) {
                queueZooKeeper.registerTopicInZk(topic);
            }
            startupLatch.countDown();
        }
        logger.info("Starting log flusher every " + config.getFlushSchedulerThreadRate() + " ms with the following overrides " + logFlushIntervalMap);
        // 启动日志刷新调度器
        logFlusherScheduler.scheduleWithRate(new Runnable() {
            @Override
            public void run() {
                flushAllLogs();
            }
        }, config.getFlushSchedulerThreadRate(), config.getFlushSchedulerThreadRate());
    }

    // 等待启动完成
    private void awaitStartup() throws InterruptedException {
        if (config.isEnableZookeeper()) {
            startupLatch.await();
        }
    }

    // 在ZooKeeper中注册新主题
    public void registerNewTopicInZK(String topic) {
        if (config.isEnableZookeeper()) {
            zkActor.send(new TopicMsg(topic));
        }
    }

    // 为给定的主题和分区创建日志
    private Log createLog(String topic, int partition) throws IOException {
        File d = new File(logDir, topic + "-" + partition);
        d.mkdirs();
        return new Log(d, maxSize, flushInterval, false);
    }

    // 选择一个随机分区
    public int chooseRandomPartition(String topic) {
        return random.nextInt(topicPartitionsMap.getOrDefault(topic, numPartitions));
    }

    // 获取或创建日志
    public Log getOrCreateLog(String topic, int partition) throws InterruptedException, IOException {
        awaitStartup();
        if (topic.length() <= 0) {
            throw new InvalidTopicException("topic name can't be empty");
        }
        if (partition < 0 || partition >= topicPartitionsMap.getOrDefault(topic, numPartitions)) {
            logger.warn("Wrong partition " + partition + " valid partitions (0," +
                    (topicPartitionsMap.getOrDefault(topic, numPartitions) - 1) + ")");
            throw new InvalidPartitionException("wrong partition " + partition);
        }
        boolean hasNewTopic = false;
        Pool<Integer, Log> parts = logs.get(topic);
        if (parts == null) {
            Pool<Integer, Log> found = logs.putIfNotExists(topic, new Pool<>());
            if (found == null) {
                hasNewTopic = true;
            }
            parts = logs.get(topic);
        }
        Log log = parts.get(partition);
        if (log == null) {
            log = createLog(topic, partition);
            Log foundLog = parts.putIfNotExists(partition, log);
            if (foundLog != null) {
                // 日志已存在
                log.close();
                log = foundLog;
            } else {
                logger.info("Created log for '" + topic + "'-" + partition);
            }
        }
        if (hasNewTopic) {
            registerNewTopicInZK(topic);
        }
        return log;
    }

    // 清理日志
    public void cleanupLogs() {
        logger.debug("Beginning log cleanup...");
        Iterator<Log> iter = getLogIterator();
        int total = 0;
        long startMs = System.currentTimeMillis();
        while (iter.hasNext()) {
            Log log = iter.next();
            logger.debug("Garbage collecting '" + log.getName() + "'");
            String topic = Utils.getTopicPartition(log.getDir().getName()).keySet().stream().findFirst().get();
            long logCleanupThresholdMS = this.logCleanupDefaultAgeMs;
            if (logRetentionMSMap.containsKey(topic)) {
                logCleanupThresholdMS = logRetentionMSMap.get(topic);
            }
            List<File> toBeDeleted = log.markDeletedWhile(startMs - _.file.lastModified > logCleanupThresholdMS);
            for (File segment : toBeDeleted) {
                logger.info("Deleting log segment " + segment.getName() + " from " + log.getName());
                if (!segment.delete()) {
                    logger.warn("Delete failed.");
                } else {
                    total++;
                }
            }
        }
        logger.debug("Log cleanup completed. " + total + " files deleted in " +
                (System.currentTimeMillis() - startMs) / 1000 + " seconds");
    }

    // 关闭所有日志
    public void close() {
        logFlusherScheduler.shutdown();
        Iterator<Log> iter = getLogIterator();
        while (iter.hasNext()) {
            iter.next().close();
        }
        if (config.isEnableZookeeper()) {
            zkActor.send(StopActor);
            queueZooKeeper.close();
        }
    }

    // 获取日志迭代器
    private Iterator<Log> getLogIterator() {
        return new Iterator<Log>() {
            private Iterator<Pool<Integer, Log>> partsIter = logs.values().iterator();
            private Iterator<Log> logIter = null;

            @Override
            public boolean hasNext() {
                while (true) {
                    if (logIter != null && logIter.hasNext()) {
                        return true;
                    }
                    if (!partsIter.hasNext()) {
                        return false;
                    }
                    logIter = partsIter.next().values().iterator();
                }
            }

            @Override
            public Log next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return logIter.next();
            }
        };
    }

    // 刷新所有日志
    private void flushAllLogs() {
        if (logger.isDebugEnabled()) {
            logger.debug("flushing the high watermark of all logs");
        }
        for (Iterator<Log> it = getLogIterator(); it.hasNext(); ) {
            Log log = it.next();
            try {
                long timeSinceLastFlush = System.currentTimeMillis() - log.getLastFlushedTime();
                long logFlushInterval = config.getDefaultFlushIntervalMs();
                if (logFlushIntervalMap.containsKey(log.getTopicName())) {
                    logFlushInterval = logFlushIntervalMap.get(log.getTopicName());
                }
                if (logger.isDebugEnabled()) {
                    logger.debug(log.getTopicName() + " flush interval " + logFlushInterval +
                            " last flushed " + log.getLastFlushedTime() + " timesincelastFlush: " + timeSinceLastFlush);
                }
                if (timeSinceLastFlush >= logFlushInterval) {
                    log.flush();
                }
            } catch (Throwable e) {
                logger.error("error flushing " + log.getTopicName(), e);
                if (e instanceof IOException) {
                    logger.error("force shutdown due to error in flushAllLogs" + e);
                    Runtime.getRuntime().halt(1);
                }
            }
        }
    }

    // 获取所有主题
    public List<String> getAllTopics() {
        return logs.keys().stream().collect(Collectors.toList());
    }

    // 获取主题分区映射
    public Map<String, Integer> getTopicPartitionsMap() {
        return topicPartitionsMap;
    }

}