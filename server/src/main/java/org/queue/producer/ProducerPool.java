package org.queue.producer;

/**
 * @author gaoyvfeng
 * @ClassName ProducerPool
 * @description:
 * @datetime 2024年 05月 24日 16:55
 * @version: 1.0
 */
import org.queue.javaapi.producer.async.EventHandler;
import org.queue.producer.async.AsyncProducer;
import org.queue.producer.async.CallbackHandler;
import org.queue.serializer.Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ProducerPool<V> {
    // 日志记录器
    private static final Logger logger = LoggerFactory.getLogger(ProducerPool.class);
    // 生产者配置
    private final ProducerConfig config;
    // 数据序列化器
    private final Encoder<V> serializer;
    // 同步生产者映射
    private final ConcurrentMap<Integer, SyncProducer> syncProducers;
    // 异步生产者映射
    private final ConcurrentMap<Integer, AsyncProducer<V>> asyncProducers;
    // 事件处理器
    private EventHandler<V> eventHandler;
    // 回调处理器
    private CallbackHandler<V> cbkHandler;
    // 是否同步生产者
    private boolean sync = true;

    public ProducerPool(ProducerConfig config, Encoder<V> serializer,
                        ConcurrentMap<Integer, SyncProducer> syncProducers,
                        ConcurrentMap<Integer, AsyncProducer<V>> asyncProducers,
                        EventHandler<V> inputEventHandler, CallbackHandler<V> cbkHandler) {
        this.config = config;
        this.serializer = serializer;
        this.syncProducers = syncProducers;
        this.asyncProducers = asyncProducers;
        this.eventHandler = inputEventHandler;
        this.cbkHandler = cbkHandler;

        initEventHandler();
        checkSerializer();
        initSync();
    }

    private void initEventHandler() {
        if (eventHandler == null) {
            eventHandler = new DefaultEventHandler<>(config, cbkHandler);
        }
    }

    private void checkSerializer() {
        if (serializer == null) {
            throw new InvalidConfigException("serializer passed in is null!");
        }
    }

    private void initSync() {
        switch (config.producerType) {
            case "sync":
                break;
            case "async":
                sync = false;
                break;
            default:
                throw new InvalidConfigException("Valid values for producer.type are sync/async");
        }
    }

    // 重载构造函数，使用默认参数
    public ProducerPool(ProducerConfig config, Encoder<V> serializer,
                        EventHandler<V> eventHandler, CallbackHandler<V> cbkHandler) {
        this(config, serializer, new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>(), eventHandler, cbkHandler);
    }

    // 重载构造函数，使用默认参数
    public ProducerPool(ProducerConfig config, Encoder<V> serializer) {
        this(config, serializer, new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>(), Utils.getObject(config.eventHandler),
                Utils.getObject(config.cbkHandler));
    }

    /**
     * 添加一个新的生产者，可以是同步或异步，连接到指定的代理。
     * @param broker 代理信息对象，包含ID、主机名和端口。
     */
    public void addProducer(Broker broker) {
        if (sync) {
            // 创建同步生产者
            Properties props = new Properties();
            props.put("host", broker.getHost()); // 设置主机名
            props.put("port", String.valueOf(broker.getPort())); // 设置端口
            props.put("buffer.size", String.valueOf(config.getBufferSize())); // 设置缓冲区大小
            props.put("connect.timeout.ms", String.valueOf(config.getConnectTimeoutMs())); // 设置连接超时
            props.put("reconnect.interval", String.valueOf(config.getReconnectInterval())); // 设置重连间隔
            SyncProducerConfig syncConfig = new SyncProducerConfig(props);
            SyncProducer producer = new SyncProducer(syncConfig);
            logger.info("Creating sync producer for broker id = " + broker.getId() + " at " + broker.getHost() + ":" + broker.getPort());
            syncProducers.put(broker.getId(), producer);
        } else {
            // 创建异步生产者
            Properties props = new Properties();
            props.put("host", broker.getHost());
            props.put("port", String.valueOf(broker.getPort()));
            props.put("queue.time", String.valueOf(config.getQueueTime())); // 设置队列时间
            props.put("queue.size", String.valueOf(config.getQueueSize())); // 设置队列大小
            props.put("batch.size", String.valueOf(config.getBatchSize())); // 设置批处理大小
            props.put("serializer.class", config.getSerializerClass()); // 设置序列化类
            ProducerConfig syncConfig = new ProducerConfig(props);
            ProducerConfig asyncConfig = new ProducerConfig(props);
            SyncProducer syncProducer = new SyncProducer(syncConfig);
            AsyncProducer<V> producer = new AsyncProducer<>(asyncConfig, syncProducer, serializer,
                    eventHandler, config.getEventHandlerProps(),
                    cbkHandler, config.getCbkHandlerProps());
            producer.start(); // 启动异步生产者
            logger.info("Creating async producer for broker id = " + broker.getId() + " at " + broker.getHost() + ":" + broker.getPort());
            asyncProducers.put(broker.getId(), producer);
        }
    }
    /**
     * 根据指定的代理ID选择同步或异步生产者，并调用选定生产者的发送API，
     * 以将数据发布到指定的代理分区。
     * @param poolDataArray 生产者池请求对象数组
     */
    public void send(ProducerPoolData<V>[] poolDataArray) {
        // 转换数组为列表以使用Java 8 Stream API
        List<ProducerPoolData<V>> poolDataList = Arrays.asList(poolDataArray);
        List<Integer> distinctBrokers = poolDataList.stream()
                .map(pd -> pd.getBidPid().brokerId)
                .distinct()
                .collect(Collectors.toList());

        // 按代理ID分区请求
        for (Integer bid : distinctBrokers) {
            List<ProducerPoolData<V>> requestsForThisBid = poolDataList.stream()
                    .filter(pd -> pd.getBidPid().brokerId == bid)
                    .collect(Collectors.toList());

            if (sync) {
                // 为同步生产者创建发送请求
                List<ProducerRequest> producerRequests = requestsForThisBid.stream()
                        .map(req -> new ProducerRequest(
                                req.getTopic(),
                                req.getBidPid().partId,
                                new ByteBufferMessageSet(
                                        config.compressionCodec,
                                        req.getData().stream()
                                                .map(d -> serializer.toMessage(d))
                                                .toArray()
                                )
                        ))
                        .collect(Collectors.toList());
                logger.debug("Fetching sync producer for broker id: " + bid);
                SyncProducer producer = syncProducers.get(bid);
                if (producer != null) {
                    if (!producerRequests.isEmpty()) {
                        producer.multiSend(producerRequests.toArray(new ProducerRequest[0]));
                    }
                    logger.debug(config.compressionCodec == NoCompressionCodec ?
                            "Sending message to broker " + bid :
                            "Sending compressed messages to broker " + bid);
                } else {
                    throw new UnavailableProducerException("Producer pool has not been initialized correctly. " +
                            "Sync Producer for broker " + bid + " does not exist in the pool");
                }
            } else {
                // 对于异步生产者，直接发送数据
                logger.debug("Fetching async producer for broker id: " + bid);
                AsyncProducer<V> producer = asyncProducers.get(bid);
                if (producer != null) {
                    for (ProducerPoolData<V> req : requestsForThisBid) {
                        for (V dataItem : req.getData()) {
                            producer.send(req.getTopic(), dataItem, req.getBidPid().partId);
                        }
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug(config.compressionCodec == NoCompressionCodec ?
                                "Sending message" :
                                "Sending compressed messages");
                    }
                } else {
                    throw new UnavailableProducerException("Producer pool has not been initialized correctly. " +
                            "Async Producer for broker " + bid + " does not exist in the pool");
                }
            }
        }
    }
    /**
     * 关闭池中的所有生产者。
     */
    public void close() {
        switch (config.getProducerType()) { // 假设config有getProducerType()方法
            case "sync":
                logger.info("Closing all sync producers");
                Iterator<SyncProducer> syncIter = syncProducers.values().iterator();
                while (syncIter.hasNext()) {
                    syncIter.next().close(); // 关闭同步生产者
                }
                break;
            case "async":
                logger.info("Closing all async producers");
                Iterator<AsyncProducer<V>> asyncIter = asyncProducers.values().iterator();
                while (asyncIter.hasNext()) {
                    asyncIter.next().close(); // 关闭异步生产者
                }
                break;
            default:
                throw new IllegalStateException("Invalid producer type: " + config.getProducerType());
        }
    }

    /**
     * 构建并返回生产者池的请求对象。
     * @param topic 主题名称，数据将被发布到此主题
     * @param bidPid 代理ID和分区ID的组合
     * @param data 要发布的数据序列
     * @return 生产者池数据对象
     */
    public ProducerPoolData<V> getProducerPoolData(String topic, Partition bidPid, Iterable<V> data) {
        return new ProducerPoolData<>(topic, bidPid, data);
    }

}
