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

package org.queue.tools

import java.net.URI

/**
 * Command line program to dump out messages to standard out using the simple consumer
 */
object SimpleConsumerShell {

  def main(args: Array[String]): Unit = {
    
    val parser = new OptionParser
    val urlOpt = parser.accepts("server", "REQUIRED: The hostname of the server to connect to.")
                           .withRequiredArg
                           .describedAs("kafka://hostname:port")
                           .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
                           .withRequiredArg
                           .describedAs("topic")
                           .ofType(classOf[String])
    val partitionOpt = parser.accepts("partition", "The partition to consume from.")
                           .withRequiredArg
                           .describedAs("partition")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(0)
    val offsetOpt = parser.accepts("offset", "The offset to start consuming from.")
                           .withRequiredArg
                           .describedAs("offset")
                           .ofType(classOf[java.lang.Long])
                           .defaultsTo(0L)
    val fetchsizeOpt = parser.accepts("fetchsize", "The fetch size of each request.")
                           .withRequiredArg
                           .describedAs("fetchsize")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1000000)

    val options = parser.parse(args : _*)
    
    for(arg <- List(urlOpt, topicOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"") 
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    val url = new URI(options.valueOf(urlOpt))
    val topic = options.valueOf(topicOpt)
    val partition = options.valueOf(partitionOpt).intValue
    val startingOffset = options.valueOf(offsetOpt).longValue
    val fetchsize = options.valueOf(fetchsizeOpt).intValue

    println("Starting consumer...")
    val consumer = new SimpleConsumer(url.getHost, url.getPort, 10000, 64*1024)
    val thread = Utils.newThread("kafka-consumer", new Runnable() {
      def run() {
        var offset = startingOffset
        while(true) {
	      val fetchRequest = new FetchRequest(topic, partition, offset, fetchsize)
	      val messageSets = consumer.multifetch(fetchRequest)
	      for (messages <- messageSets) {
	        println("multi fetched " + messages.sizeInBytes + " bytes from offset " + offset)
            var consumed = 0
            for(messageAndOffset <- messages) {
              println("consumed: " + Utils.toString(messageAndOffset.message.payload, "UTF-8"))
              consumed += 1
	        }
	        if(consumed > 0)
	          offset += messages.validBytes
          }
          Thread.sleep(10000)
        }
      }  
    }, false);
    thread.start()
    thread.join()
  }
  
}
