//package org.queue.javaapi.producer;
//
//import org.queue.producer.ProducerConfig;
//import org.queue.producer.ProducerPool;
//import org.queue.producer.Partitioner;
//import org.queue.serializer.Encoder;
//import org.queue.utils.Utils;
//
//import java.util.List;
//
//public class ProducerApi<K, V> {
//    private org.queue.producer.Producer<K, V> underlying;
//
//    public ProducerApi(ProducerConfig config, Partitioner<K> partitioner, ProducerPool<V> producerPool,
//                       boolean populateProducerPool) {
//        this.underlying = new org.queue.producer.Producer<K, V>(config, partitioner, producerPool, populateProducerPool, null);
//        // 其他初始化代码...
//    }
//
//    public ProducerApi(ProducerConfig config) {
//        this(config, Utils.getObject(config.getPartitionerClass()), new ProducerPool<V>(config, Utils.getObject(config.getSerializerClass())));
//        // 其他初始化代码...
//    }
//
//    public ProducerApi(ProducerConfig config, Encoder<V> encoder, EventHandler<V> eventHandler,
//                       CallbackHandler<V> cbkHandler, Partitioner<K> partitioner) {
//        this(config, partitioner, new ProducerPool<V>(config, encoder,
//                new EventHandlerWrapper<>(eventHandler), new CallbackHandlerWrapper<>(cbkHandler)));
//        // 其他初始化代码...
//    }
//
//    public void send(ProducerData<K, V> producerData) {
//        this.underlying.send(new org.queue.producer.ProducerData<K, V>(producerData.getTopic(), producerData.getKey(),
//                producerData.getData().toSeq()));
//    }
//
//    public void send(List<ProducerData<K, V>> producerDataList) {
//        for (ProducerData<K, V> data : producerDataList) {
//            this.send(data);
//        }
//    }
//
//    public void close() {
//        this.underlying.close();
//    }
//
//}
