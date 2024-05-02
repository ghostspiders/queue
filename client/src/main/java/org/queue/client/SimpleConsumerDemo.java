package org.queue.client;


import java.util.ArrayList;
import java.util.List;

import org.queue.javaapi.Implicits;
import org.queue.javaapi.MultiFetchResponse;
import org.queue.javaapi.consumer.SimpleConsumer;
import org.queue.javaapi.message.ByteBufferMessageSet;
import org.queue.message.MessageAndOffset;
import org.queue.api.FetchRequest;
import org.queue.message.MessageSet;
import scala.collection.Iterator;
public class SimpleConsumerDemo
{
    private static void printMessages(ByteBufferMessageSet messageSet){

        MessageSet iterable = messageSet.toIterable();
        Iterator<MessageAndOffset> iterator = iterable.iterator();
        while (iterator.hasNext()){
            System.out.println(ExampleUtils.getMessage(iterator.next().message()));
            System.out.println(iterator.next());
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
        org.queue.message.ByteBufferMessageSet messageSet = simpleConsumer.fetch(req);
        printMessages(Implicits.scalaMessageSetToJavaMessageSet(messageSet));

        System.out.println("Testing single multi-fetch");
        req = new FetchRequest(KafkaProperties.topic2, 0, 0L, 100);
        List<FetchRequest> list = new ArrayList<FetchRequest>();
        list.add(req);
        req = new FetchRequest(KafkaProperties.topic3, 0, 0L, 100);
        list.add(req);

        org.queue.api.MultiFetchResponse multifetch = simpleConsumer.multifetch(list);
        MultiFetchResponse response = Implicits.toJavaMultiFetchResponse(multifetch);
        int fetchReq = 0;


        java.util.Iterator<org.queue.message.ByteBufferMessageSet> iterator1 = response.iterator();
        while (iterator1.hasNext()){
            System.out.println("Response from fetch request no: " + ++fetchReq);
            printMessages(Implicits.scalaMessageSetToJavaMessageSet(iterator1.next()));
        }
    }

}

