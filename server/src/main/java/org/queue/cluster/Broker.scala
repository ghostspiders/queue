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

package org.queue.cluster

import org.queue.utils.Utils

/**
 * A Kafka broker
 */
private[queue] object Broker {
  def createBroker(id: Int, brokerInfoString: String): Broker = {
    val brokerInfo = brokerInfoString.split(":")
    new Broker(id, brokerInfo(0), brokerInfo(1), brokerInfo(2).toInt)
  }
}

private[queue] class Broker(val id: Int, val creatorId: String, val host: String, val port: Int) {
  
  override def toString(): String = new String("id:" + id + ",creatorId:" + creatorId + ",host:" + host + ",port:" + port)

  def getZKString(): String = new String(creatorId + ":" + host + ":" + port)
  
  override def equals(obj: Any): Boolean = {
    obj match {
      case null => false
      case n: Broker => id == n.id && host == n.host && port == n.port
      case _ => false
    }
  }
  
  override def hashCode(): Int = Utils.hashcode(id, host, port)
  
}
