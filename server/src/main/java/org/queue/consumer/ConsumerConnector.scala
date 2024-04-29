/*
 * Copyright 2010 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.queue.consumer

import org.apache.logging.log4j.LogManager
import org.queue.utils.Utils

/**
 *  Main interface for consumer
 */
trait ConsumerConnector {
  /**
   *  Create a list of MessageStreams for each topic.
   *
   *  @param topicCountMap  a map of (topic, #streams) pair
   *  @return a map of (topic, list of  QueueMessageStream) pair. The number of items in the
   *          list is #streams. Each KafkaMessageStream supports an iterator of messages.
   */
  def createMessageStreams(topicCountMap: Map[String,Int]) : Map[String,List[KafkaMessageStream]]

  /**
   *  Commit the offsets of all broker partitions connected by this connector.
   */
  def commitOffsets()
  
  /**
   *  Shut down the connector
   */
  def shutdown()
}

object Consumer {
  private val logger = LogManager.getLogger(getClass())
  private val consumerStatsMBeanName = "kafka:type=kafka.ConsumerStats"

  /**
   *  Create a ConsumerConnector
   *
   *  @param config  at the minimum, need to specify the groupid of the consumer and the zookeeper
   *                 connection string zk.connect.
   */
  def create(config: ConsumerConfig): ConsumerConnector = {
    val consumerConnect = new ZookeeperConsumerConnector(config)
    Utils.swallow(logger.warn, Utils.registerMBean(consumerConnect, consumerStatsMBeanName))
    consumerConnect
  }

  /**
   *  Create a ConsumerConnector
   *
   *  @param config  at the minimum, need to specify the groupid of the consumer and the zookeeper
   *                 connection string zk.connect.
   */
  def createJavaConsumerConnector(config: ConsumerConfig): org.queue.javaapi.consumer.ConsumerConnector = {
    val consumerConnect = new org.queue.javaapi.consumer.ZookeeperConsumerConnector(config)
    Utils.swallow(logger.warn, Utils.registerMBean(consumerConnect.underlying, consumerStatsMBeanName))
    consumerConnect
  }
}
