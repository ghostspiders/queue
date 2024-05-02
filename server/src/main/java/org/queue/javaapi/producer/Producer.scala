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

package org.queue.javaapi.producer
import org.queue.utils.Utils
import org.queue.producer.async.QueueItem

import java.util.Properties
import org.queue.producer.{Partitioner, ProducerConfig, ProducerPool}
import org.queue.serializer.Encoder

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.jdk.CollectionConverters.ListHasAsScala

class Producer[K,V](config: ProducerConfig,
                    partitioner: Partitioner[K],
                    producerPool: ProducerPool[V],
                    populateProducerPool: Boolean = true)
{

  private val underlying = new org.queue.producer.Producer[K,V](config, partitioner, producerPool, populateProducerPool, null)

  /**
   * This constructor can be used when all config parameters will be specified through the
   * ProducerConfig object
   * @param config Producer Configuration object
   */
  def this(config: ProducerConfig) = this(config, Utils.getObject(config.partitionerClass),
    new ProducerPool[V](config, Utils.getObject(config.serializerClass)))


  def this(config: ProducerConfig,
           encoder: Encoder[V],
           eventHandler: org.queue.javaapi.producer.async.EventHandler[V],
           cbkHandler: org.queue.javaapi.producer.async.CallbackHandler[V],
           partitioner: Partitioner[K]) = {
    this(config, partitioner,
      new ProducerPool[V](config, encoder,
        new org.queue.producer.async.EventHandler[V] {
          override def init(props: Properties) { eventHandler.init(props) }
          override def handle(events: Seq[QueueItem[V]], producer: org.queue.producer.SyncProducer,
                              encoder: Encoder[V]) {
            import org.queue.javaapi.Implicits._
            eventHandler.handle(events, producer, encoder)
          }
          override def close { eventHandler.close }
        },
        new org.queue.producer.async.CallbackHandler[V] {
          override def init(props: Properties) { cbkHandler.init(props)}
          override def beforeEnqueue(data: QueueItem[V] = null.asInstanceOf[QueueItem[V]]): QueueItem[V] = {
            cbkHandler.beforeEnqueue(data)
          }
          override def afterEnqueue(data: QueueItem[V] = null.asInstanceOf[QueueItem[V]], added: Boolean) {
            cbkHandler.afterEnqueue(data, added)
          }
          override def afterDequeuingExistingData(data: QueueItem[V] = null): scala.collection.mutable.Seq[QueueItem[V]] = {
            cbkHandler.afterDequeuingExistingData(data).asScala
          }
          override def beforeSendingData(data: Seq[QueueItem[V]] = null): scala.collection.mutable.Seq[QueueItem[V]] = {
            import org.queue.javaapi.Implicits._
            cbkHandler.beforeSendingData(data)
          }
          override def lastBatchBeforeClose: scala.collection.mutable.Seq[QueueItem[V]] = {
            cbkHandler.lastBatchBeforeClose.asScala
          }
          override def close { cbkHandler.close }
        }))
  }

  /**
   * Sends the data to a single topic, partitioned by key, using either the
   * synchronous or the asynchronous producer
   * @param producerData the producer data object that encapsulates the topic, key and message data
   */
  def send(producerData: org.queue.javaapi.producer.ProducerData[K,V]) {
    underlying.send(new org.queue.producer.ProducerData[K,V](producerData.getTopic, producerData.getKey,
      producerData.getData.toSeq))
  }

  /**
   * Use this API to send data to multiple topics
   * @param producerData list of producer data objects that encapsulate the topic, key and message data
   */
  def send(producerData: java.util.List[org.queue.javaapi.producer.ProducerData[K,V]]) {
    for (elem <- producerData) {
      send(elem)
    }
  }

  /**
   * Close API to close the producer pool connections to all Kafka brokers. Also closes
   * the zookeeper client connection if one exists
   */
  def close = underlying.close
}
