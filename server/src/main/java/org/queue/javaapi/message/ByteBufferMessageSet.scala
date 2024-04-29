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
package org.queue.javaapi.message

import org.queue.common.ErrorMapping
import org.queue.message.{CompressionCodec, CompressionUtils, Message, MessageAndOffset, MessageSet, NoCompressionCodec}
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import java.nio.channels.WritableByteChannel
import scala.jdk.CollectionConverters.CollectionHasAsScala

class ByteBufferMessageSet(private val buffer: ByteBuffer,
                           private val initialOffset: Long = 0L,
                           private val errorCode: Int = ErrorMapping.NoError) extends MessageSet {
  private val logger = LoggerFactory.getLogger(getClass())
  val underlying: org.queue.message.ByteBufferMessageSet = new org.queue.message.ByteBufferMessageSet(buffer,
                                                                                              initialOffset,
                                                                                              errorCode)
  def this(buffer: ByteBuffer) = this(buffer, 0L, ErrorMapping.NoError)

  def this(compressionCodec: CompressionCodec, messages: java.util.List[Message]) {
    this(compressionCodec match {
      case NoCompressionCodec =>
        val buffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages))
        val messageIterator = messages.iterator
        while(messageIterator.hasNext) {
          val message = messageIterator.next
          message.serializeTo(buffer)
        }
        buffer.rewind
        buffer
      case _ =>

        val message = CompressionUtils.compress(messages.asScala, compressionCodec)
        val buffer = ByteBuffer.allocate(message.serializedSize)
        message.serializeTo(buffer)
        buffer.rewind
        buffer
    }, 0L, ErrorMapping.NoError)
  }

  def this(messages: java.util.List[Message]) {
    this(NoCompressionCodec, messages)
  }

  def validBytes: Long = underlying.validBytes

  def serialized():ByteBuffer = underlying.serialized

  def getInitialOffset = initialOffset

  def getBuffer = buffer

  def getErrorCode = errorCode

  override def iterator: Iterator[MessageAndOffset] = new Iterator[MessageAndOffset] {
    val underlyingIterator = underlying.iterator
    override def hasNext: Boolean = {
      underlyingIterator.hasNext
    }
    override def next(): MessageAndOffset = {
      underlyingIterator.next
    }
  }

  override def toString: String = underlying.toString

  def sizeInBytes(): Long = underlying.sizeInBytes

  override def equals(other: Any): Boolean = {
    other match {
      case that: ByteBufferMessageSet =>
        (that canEqual this) && errorCode == that.errorCode && buffer.equals(that.buffer) && initialOffset == that.initialOffset
      case _ => false
    }
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[ByteBufferMessageSet]

  override def hashCode: Int = 31 * (17 + errorCode) + buffer.hashCode + initialOffset.hashCode

  /** Write the messages in this set to the given channel starting at the given offset byte.
   * Less than the complete amount may be written, but no more than maxSize can be. The number
   * of bytes written is returned */
  override def writeTo(channel: WritableByteChannel, offset: Long, maxSize: Long): Long = {
    offset
  }
}
