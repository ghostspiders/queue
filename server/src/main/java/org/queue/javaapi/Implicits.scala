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
package org.queue.javaapi

import org.apache.logging.log4j.LogManager
import org.queue.javaapi.message.ByteBufferMessageSet
import org.queue.producer.async.QueueItem
import org.queue.serializer.Encoder

import java.util.Arrays.asList
import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.jdk.CollectionConverters.{ SeqHasAsJava}

private[javaapi] object Implicits {
  private val logger = LogManager.getLogger(getClass())

  implicit def javaMessageSetToScalaMessageSet(messageSet: ByteBufferMessageSet):
     org.queue.message.ByteBufferMessageSet = messageSet.underlying

  implicit def scalaMessageSetToJavaMessageSet(messageSet: org.queue.message.ByteBufferMessageSet):
  org.queue.javaapi.message.ByteBufferMessageSet = {
    new org.queue.javaapi.message.ByteBufferMessageSet(messageSet.getBuffer, messageSet.getInitialOffset,
                                                   messageSet.getErrorCode)
  }

  implicit def toJavaSyncProducer(producer: org.queue.producer.SyncProducer): org.queue.javaapi.producer.SyncProducer = {
    if(logger.isDebugEnabled)
      logger.debug("Implicit instantiation of Java Sync Producer")
    new org.queue.javaapi.producer.SyncProducer(producer)
  }

  implicit def toSyncProducer(producer: org.queue.javaapi.producer.SyncProducer): org.queue.producer.SyncProducer = {
    if(logger.isDebugEnabled)
      logger.debug("Implicit instantiation of Sync Producer")
    producer.underlying
  }

  implicit def toScalaEventHandler[T](eventHandler: org.queue.javaapi.producer.async.EventHandler[T]) : org.queue.producer.async.EventHandler[T] = {
    new org.queue.producer.async.EventHandler[T] {
      override def init(props: java.util.Properties) { eventHandler.init(props) }
      override def handle(events: Seq[QueueItem[T]], producer: org.queue.producer.SyncProducer, encoder: Encoder[T]) {
        eventHandler.handle(events.asJava, producer, encoder)
      }
      override def close { eventHandler.close }
    }
  }

  implicit def toJavaEventHandler[T](eventHandler: org.queue.producer.async.EventHandler[T])
    : org.queue.javaapi.producer.async.EventHandler[T] = {
    new org.queue.javaapi.producer.async.EventHandler[T] {
      override def init(props: java.util.Properties) { eventHandler.init(props) }
      override def handle(events: java.util.List[QueueItem[T]], producer: org.queue.javaapi.producer.SyncProducer,
                          encoder: Encoder[T]) {
        eventHandler.handle(events.toSeq, producer, encoder)
      }
      override def close { eventHandler.close }
    }
  }

  implicit def toScalaCbkHandler[T](cbkHandler: org.queue.javaapi.producer.async.CallbackHandler[T])
      : org.queue.producer.async.CallbackHandler[T] = {
    new org.queue.producer.async.CallbackHandler[T] {
      override def init(props: java.util.Properties) { cbkHandler.init(props)}
      override def beforeEnqueue(data: QueueItem[T] = null.asInstanceOf[QueueItem[T]]): QueueItem[T] = {
        cbkHandler.beforeEnqueue(data)
      }
      override def afterEnqueue(data: QueueItem[T] = null.asInstanceOf[QueueItem[T]], added: Boolean) {
        cbkHandler.afterEnqueue(data, added)
      }
      override def afterDequeuingExistingData(data: QueueItem[T] = null): scala.collection.mutable.Seq[QueueItem[T]] = {
        cbkHandler.afterDequeuingExistingData(data)
      }
      override def beforeSendingData(data: Seq[QueueItem[T]] = null): scala.collection.mutable.Seq[QueueItem[T]] = {
        cbkHandler.beforeSendingData(data.asJava)
      }
      override def lastBatchBeforeClose: scala.collection.mutable.Seq[QueueItem[T]] = {
        cbkHandler.lastBatchBeforeClose
      }
      override def close { cbkHandler.close }
    }
  }

  implicit def toJavaCbkHandler[T](cbkHandler: org.queue.producer.async.CallbackHandler[T])
      : org.queue.javaapi.producer.async.CallbackHandler[T] = {
    new org.queue.javaapi.producer.async.CallbackHandler[T] {
      override def init(props: java.util.Properties) { cbkHandler.init(props)}
      override def beforeEnqueue(data: QueueItem[T] = null.asInstanceOf[QueueItem[T]]): QueueItem[T] = {
        cbkHandler.beforeEnqueue(data)
      }
      override def afterEnqueue(data: QueueItem[T] = null.asInstanceOf[QueueItem[T]], added: Boolean) {
        cbkHandler.afterEnqueue(data, added)
      }
      override def afterDequeuingExistingData(data: QueueItem[T] = null) : java.util.List[QueueItem[T]] = {
        cbkHandler.afterDequeuingExistingData(data).asJava
      }
      override def beforeSendingData(data: java.util.List[QueueItem[T]] = null) : java.util.List[QueueItem[T]] = {
        cbkHandler.beforeSendingData(data.toSeq).asJava
      }
      override def lastBatchBeforeClose: java.util.List[QueueItem[T]] = {
        cbkHandler.lastBatchBeforeClose.asJava
    }
      override def close { cbkHandler.close }
    }
  }

  implicit def toMultiFetchResponse(response: org.queue.javaapi.MultiFetchResponse): org.queue.api.MultiFetchResponse =
    response.underlying

  implicit def toJavaMultiFetchResponse(response: org.queue.api.MultiFetchResponse): org.queue.javaapi.MultiFetchResponse =
    new org.queue.javaapi.MultiFetchResponse(response.buffer, response.numSets, response.offsets)
}
