//package org.queue.client;
//
///*
// * Copyright 2010 LinkedIn
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//import java.util.ArrayList;
//import java.util.List;
//
//import org.queue.javaapi.MultiFetchResponse;
//import org.queue.javaapi.consumer.SimpleConsumer;
//import org.queue.javaapi.message.ByteBufferMessageSet;
//import org.queue.message.MessageAndOffset;
//import scala.collection.Iterator;
//
//import org.queue.api.FetchRequest;
//
//public class SimpleProducerDemo {
//    private static void printMessages(ByteBufferMessageSet messageSet) {
//        Iterator<MessageAndOffset> iterator = messageSet.toIterable().iterator();
//        while (iterator.hasNext()){
//            System.out.println(ExampleUtils.getMessage(iterator.next().message()));
//        }
//    }
//
//    private static void generateData() {
//        Producer producer2 = new Producer(KafkaProperties.topic2);
//        producer2.start();
//        Producer producer3 = new Producer(KafkaProperties.topic3);
//        producer3.start();
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public static void main(String[] args) {
//
//        generateData();
//        SimpleConsumer simpleConsumer = new SimpleConsumer(KafkaProperties.kafkaServerURL,
//                KafkaProperties.kafkaServerPort,
//                KafkaProperties.connectionTimeOut,
//                KafkaProperties.kafkaProducerBufferSize);
//
//        System.out.println("Testing single fetch");
//        FetchRequest req = new FetchRequest(KafkaProperties.topic2, 0, 0L, 100);
//        ByteBufferMessageSet messageSet = simpleConsumer.fetch(req);
//        printMessages(messageSet);
//
//        System.out.println("Testing single multi-fetch");
//        req = new FetchRequest(KafkaProperties.topic2, 0, 0L, 100);
//        List<FetchRequest> list = new ArrayList<FetchRequest>();
//        list.add(req);
//        req = new FetchRequest(KafkaProperties.topic3, 0, 0L, 100);
//        list.add(req);
//        MultiFetchResponse response = simpleConsumer.multifetch(list);
//        int fetchReq = 0;
//        for (ByteBufferMessageSet resMessageSet : response ) {
//            System.out.println("Response from fetch request no: " + ++fetchReq);
//            printMessages(resMessageSet);
//        }
//    }
//
//}
