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

import org.queue.javaapi.message.ByteBufferMessageSet
import org.queue.producer.SyncProducerConfig
import org.queue.javaapi.Implicits._
class SyncProducer(syncProducer: org.queue.producer.SyncProducer) {

  def this(config: SyncProducerConfig) = this(new org.queue.producer.SyncProducer(config))

  val underlying = syncProducer

  def send(topic: String, partition: Int, messages: ByteBufferMessageSet) {
    underlying.send(topic, partition, messages)
  }

  def send(topic: String, messages: ByteBufferMessageSet): Unit = send(topic,org.queue.api.ProducerRequest.RandomPartition,
    messages)

  def multiSend(produces: Array[org.queue.javaapi.ProducerRequest]) {

    val produceRequests = new Array[org.queue.api.ProducerRequest](produces.length)
    for(i <- 0 until produces.length)
      produceRequests(i) = new org.queue.api.ProducerRequest(produces(i).topic, produces(i).partition, produces(i).messages)
    underlying.multiSend(produceRequests)
  }

  def close() {
    underlying.close
  }
}
