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

import org.queue.utils.Utils
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

/**
 *  Main interface for consumer
 */
trait ConsumerConnector {

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
  private val logger = LoggerFactory.getLogger(getClass())
  private val consumerStatsMBeanName = "queue:type=queue.ConsumerStats"

  /**
   *  Create a ConsumerConnector
   *
   *  @param config  at the minimum, need to specify the groupid of the consumer and the zookeeper
   *                 connection string zk.connect.
   */
  def create(config: ConsumerConfig): ConsumerConnector = {
    val consumerConnect = new ZookeeperConsumerConnector(config)
    Utils.swallow(Level.TRACE,Utils.registerMBean(consumerConnect, consumerStatsMBeanName))
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
    Utils.swallow(Level.WARN, Utils.registerMBean(consumerConnect.underlying, consumerStatsMBeanName))
    consumerConnect
  }
}
