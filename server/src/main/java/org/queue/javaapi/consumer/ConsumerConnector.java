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
//*/
//
//package org.queue.javaapi.consumer;
//
//import org.queue.consumer.QueueMessageStream;
//
//import java.util.List;
//import java.util.Map;
//
//public interface ConsumerConnector {
//
//     Map<String, List<QueueMessageStream>> createMessageStreams(Map<String, Integer> topicCountMap);
//
//    /**
//     *  Commit the offsets of all broker partitions connected by this connector.
//     */
//     void commitOffsets();
//
//    /**
//     *  Shut down the connector
//     */
//     void shutdown();
//}
