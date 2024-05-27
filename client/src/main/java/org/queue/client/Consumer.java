package org.queue.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.queue.consumer.ConsumerConfig;
import org.queue.consumer.ConsumerIterator;
import org.queue.consumer.QueueMessageStream;
import org.queue.javaapi.consumer.ConsumerConnector;

public class Consumer extends Thread {
    private final ConsumerConnector consumer;
    private final String topic;

    public Consumer(String topic) {
        consumer = org.queue.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
        this.topic = topic;
    }

    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zk.connect", QueueProperties.zkConnect);
        props.put("groupid", QueueProperties.groupId);
        props.put("zk.sessiontimeout.ms", "400");
        props.put("zk.synctime.ms", "200");
        props.put("autocommit.interval.ms", "1000");

        return new ConsumerConfig(props);

    }

    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<QueueMessageStream>> consumerMap = consumer.createMessageStreams(topicCountMap);
        QueueMessageStream stream =  consumerMap.get(topic).get(0);
        ConsumerIterator it = stream.iterator();
        while(it.hasNext()){
            System.out.println("ConsumerConsole"+ExampleUtils.getMessage(it.next()));
        }
    }
}
