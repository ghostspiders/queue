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

import com.google.gson.Gson
import org.slf4j.LoggerFactory

import scala.collection.{Map, mutable}
import scala.jdk.CollectionConverters.MapHasAsScala

private[consumer] object TopicCount {
  private val logger = LoggerFactory.getLogger(getClass())
  val gson = new Gson()
  def constructTopicCount(consumerIdSting: String, jsonString : String) : TopicCount = {
    if("{  }".equals(jsonString)){
      return new TopicCount(consumerIdSting, gson.fromJson("", classOf[java.util.Map[String, Int]]))
    }

    var value :java.util.Map[String, Int] = null
    try {
      value = gson.fromJson(jsonString, classOf[java.util.Map[String, Int]])
      if(value == null){
        throw new RuntimeException("error constructing TopicCount : " + jsonString)
      }
    }catch {
      case e : Throwable =>
        logger.error("error parsing consumer json string " + jsonString, e)
        throw e
    }
    new TopicCount(consumerIdSting, value.asScala)
  }

}

private[consumer] class TopicCount(val consumerIdString: String, val topicCountMap: Map[String, Int]) {

  def getConsumerThreadIdsPerTopic(): Map[String, Set[String]] = {
    val consumerThreadIdsPerTopicMap = new mutable.HashMap[String, Set[String]]()
    for ((topic, nConsumers) <- topicCountMap) {
      val consumerSet = new mutable.HashSet[String]
      assert(nConsumers >= 1)
      for (i <- 0 until nConsumers)
        consumerSet += consumerIdString + "-" + i
      consumerThreadIdsPerTopicMap.put(topic, consumerSet.toSet)
    }
    consumerThreadIdsPerTopicMap
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case null => false
      case n: TopicCount => consumerIdString == n.consumerIdString && topicCountMap == n.topicCountMap
      case _ => false
    }
  }

  /**
   *  return json of
   *  { "topic1" : 4,
   *    "topic2" : 4
   *  }
   */
  def toJsonString() : String = {
    val builder = new StringBuilder
    builder.append("{ ")
    var i = 0
    for ( (topic, nConsumers) <- topicCountMap) {
      if (i > 0)
        builder.append(",")
      builder.append("\"" + topic + "\": " + nConsumers)
      i += 1
    }
    builder.append(" }")
    builder.toString
  }
}
