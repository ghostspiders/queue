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

package org.queue.serializer

import org.queue.message.Message

trait Encoder[T] {
  def toMessage(event: T):Message
}

class DefaultEncoder extends Encoder[Message] {
  override def toMessage(event: Message):Message = event
}

class StringEncoder extends Encoder[String] {
  override def toMessage(event: String):Message = new Message(event.getBytes)
}
