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

package org.queue

import org.queue.consumer.ConsumerConfig
import org.queue.server.{KafkaConfig, KafkaServerStartable}
import org.queue.utils.Utils
import org.slf4j.LoggerFactory

object Queue {
  private val logger = LoggerFactory.getLogger(Queue.getClass)

  def main(args: Array[String]): Unit = {
//    val kafkaLog4jMBeanName = "kafka:type=kafka.KafkaLog4j"
//    Utils.swallow(Level.WARN, Utils.registerMBean(LoggerFactory.getLogger(""), kafkaLog4jMBeanName))

    var serverPath = System.getProperty("server.config")
    if(serverPath == null || serverPath.isBlank) {
      serverPath = getClass().getResource("/server.properties").getPath
    }
    var consumerPath = System.getProperty("consumer.config")
    if(consumerPath == null || serverPath.isBlank) {
      consumerPath = getClass().getResource("/consumer.properties").getPath
    }

    try {
      var kafkaServerStartble: KafkaServerStartable = null
      val props = Utils.loadProps(serverPath)
      val serverConfig = new KafkaConfig(props)
      if (args.length == 2) {
        val consumerConfig = new ConsumerConfig(Utils.loadProps(consumerPath))
        kafkaServerStartble = new KafkaServerStartable(serverConfig, consumerConfig)
      }else {
        kafkaServerStartble = new KafkaServerStartable(serverConfig)
      }

      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        override def run() = {
          kafkaServerStartble.shutdown
          kafkaServerStartble.awaitShutdown
        }
      });

      kafkaServerStartble.startup
      kafkaServerStartble.awaitShutdown
    }catch {
      case e : Throwable => logger.error(e.getMessage,e)
    }
    System.exit(0)
  }
}
