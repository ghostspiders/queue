package org.queue.client;


import java.util.ArrayList;
import java.util.List;

import org.queue.javaapi.MultiFetchResponse;
import org.queue.javaapi.consumer.SimpleConsumer;
import org.queue.javaapi.message.ByteBufferMessageSet;
import org.queue.message.MessageAndOffset;
import scala.collection.Iterator;

import org.queue.api.FetchRequest;
import org.queue.message.Message;


public class SimpleConsumerDemo
{
    private static void printMessages(ByteBufferMessageSet messageSet)
    {
        for (MessageAndOffset messageAndOffset : messageSet) {
            System.out.println(ExampleUtils.getMessage(messageAndOffset.message()));
        }
    }

    private static void generateData()
    {
        Producer producer2 = new Producer(KafkaProperties.topic2);
        producer2.start();
        Producer producer3 = new Producer(KafkaProperties.topic3);
        producer3.start();
        try
        {
            Thread.sleep(1000);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    public static void main(String[] args)
    {

        generateData();
        SimpleConsumer simpleConsumer = new SimpleConsumer(KafkaProperties.kafkaServerURL,
                KafkaProperties.kafkaServerPort,
                KafkaProperties.connectionTimeOut,
                KafkaProperties.kafkaProducerBufferSize);

        System.out.println("Testing single fetch");
        FetchRequest req = new FetchRequest(KafkaProperties.topic2, 0, 0L, 100);
        ByteBufferMessageSet messageSet = simpleConsumer.fetch(req);
        printMessages(messageSet);

        System.out.println("Testing single multi-fetch");
        req = new FetchRequest(KafkaProperties.topic2, 0, 0L, 100);
        List<FetchRequest> list = new ArrayList<FetchRequest>();
        list.add(req);
        req = new FetchRequest(KafkaProperties.topic3, 0, 0L, 100);
        list.add(req);
        MultiFetchResponse response = simpleConsumer.multifetch(list);
        int fetchReq = 0;
        for (ByteBufferMessageSet resMessageSet : response )
        {
            System.out.println("Response from fetch request no: " + ++fetchReq);
            printMessages(resMessageSet);
        }
    }

}

