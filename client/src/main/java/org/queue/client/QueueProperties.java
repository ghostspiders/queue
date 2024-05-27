package org.queue.client;

public interface QueueProperties {
    final static String zkConnect = "127.0.0.1:2181";
    final static  String groupId = "group1";
    final static String topic = "topic1";
    final static String QueueServerURL = "localhost";
    final static int QueueServerPort = 9092;
    final static int QueueProducerBufferSize = 64*1024;
    final static int connectionTimeOut = 100000;
    final static int reconnectInterval = 10000*10000;
    final static String topic2 = "topic2";
    final static String topic3 = "topic3";
}
