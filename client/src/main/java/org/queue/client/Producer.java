package org.queue.client;


import org.queue.javaapi.producer.ProducerData;
import org.queue.producer.ProducerConfig;
import java.util.Properties;

public class Producer extends Thread
{
    private final org.queue.javaapi.producer.Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();

    public Producer(String topic)
    {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("zk.connect", "localhost:2181");
        // Use random partitioner. Don't need the key type. Just set it to Integer.
        // The message is of type String.
        producer = new org.queue.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
        this.topic = topic;
    }

    public void run() {
        int messageNo = 1;
        while(true)
        {
            String messageStr = new String("Message_" + messageNo);
            producer.send(new ProducerData<Integer, String>(topic, messageStr));
            messageNo++;
        }
    }

}

