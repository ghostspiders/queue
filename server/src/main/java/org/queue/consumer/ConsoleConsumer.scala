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

import jdk.internal.joptsimple.{OptionException, OptionParser, OptionSet, OptionSpec}
import org.I0Itec.zkclient.ZkClient
import org.queue.message.Message
import org.queue.utils.{StringSerializer, Utils}
import org.slf4j.LoggerFactory

import java.io.PrintStream
import java.util.{Properties, Random}
import scala.collection.convert.ImplicitConversions.`seq AsJavaList`
import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
 * Consumer that dumps messages out to standard out.
 *
 */
object ConsoleConsumer {
  
  private val logger = LoggerFactory.getLogger(getClass())

  def main(args: Array[String]) {
    val parser = new OptionParser
    val topicIdOpt = parser.accepts("topic", "REQUIRED: The topic id to consume on.")
                           .withRequiredArg
                           .describedAs("topic")
                           .ofType(classOf[String])
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " + 
                                      "Multiple URLS can be given to allow fail-over.")
                           .withRequiredArg
                           .describedAs("urls")
                           .ofType(classOf[String])
    val groupIdOpt = parser.accepts("group", "The group id to consume on.")
                           .withRequiredArg
                           .describedAs("gid")
                           .defaultsTo("console-consumer-" + new Random().nextInt(100000))   
                           .ofType(classOf[String])
    val fetchSizeOpt = parser.accepts("fetch-size", "The amount of data to fetch in a single request.")
                           .withRequiredArg
                           .describedAs("size")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1024 * 1024)   
    val socketBufferSizeOpt = parser.accepts("socket-buffer-size", "The size of the tcp RECV size.")
                           .withRequiredArg
                           .describedAs("size")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(2 * 1024 * 1024)
    val messageFormatterOpt = parser.accepts("formatter", "The name of a class to use for formatting kafka messages for display.")
                           .withRequiredArg
                           .describedAs("class")
                           .ofType(classOf[String])
                           .defaultsTo(classOf[NewlineMessageFormatter].getName)
    val messageFormatterArgOpt = parser.accepts("property")
                           .withRequiredArg
                           .describedAs("prop")
                           .ofType(classOf[String])
    val resetBeginningOpt = parser.accepts("from-beginning", "If the consumer does not already have an established offset to consume from, " +
    		"start with the earliest message present in the log rather than the latest message.")
    val autoCommitIntervalOpt = parser.accepts("autocommit.interval.ms", "The time interval at which to save the current offset in ms")
                           .withRequiredArg
                           .describedAs("ms")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(10*1000)
    val maxMessagesOpt = parser.accepts("max-messages", "The maximum number of messages to consume before exiting. If not set, consumption is continual.")
                           .withRequiredArg
                           .describedAs("num_messages")
                           .ofType(classOf[java.lang.Integer])
    val skipMessageOnErrorOpt = parser.accepts("skip-message-on-error", "If there is an error when processing a message, " +
    		"skip it instead of halt.")

    val options: OptionSet = tryParse(parser, args)
    checkRequiredArgs(parser, options, topicIdOpt, zkConnectOpt)
    
    val props = new Properties()
    props.put("groupid", options.valueOf(groupIdOpt))
    props.put("socket.buffer.size", options.valueOf(socketBufferSizeOpt).toString)
    props.put("fetch.size", options.valueOf(fetchSizeOpt).toString)
    props.put("auto.commit", "true")
    props.put("autocommit.interval.ms", options.valueOf(autoCommitIntervalOpt).toString)
    props.put("autooffset.reset", if(options.has(resetBeginningOpt)) "smallest" else "largest")
    props.put("zk.connect", options.valueOf(zkConnectOpt))
    val config = new ConsumerConfig(props)
    val skipMessageOnError = if (options.has(skipMessageOnErrorOpt)) true else false
    
    val topic = options.valueOf(topicIdOpt)
    val messageFormatterClass = Class.forName(options.valueOf(messageFormatterOpt))
    val formatterArgs: Properties = tryParseFormatterArgs(options.valuesOf(messageFormatterArgOpt).asScala)
    
    val maxMessages = if(options.has(maxMessagesOpt)) options.valueOf(maxMessagesOpt).intValue else -1

    val connector = Consumer.create(config)
    
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        connector.shutdown()
        // if there is no group specified then avoid polluting zookeeper with persistent group data, this is a hack
        if(!options.has(groupIdOpt))  
          tryCleanupZookeeper(options.valueOf(zkConnectOpt), options.valueOf(groupIdOpt))
      }
    })
    
    var stream: KafkaMessageStream = connector.createMessageStreams(Map(topic -> 1)).get(topic).get.get(0)
    val iter =
      if(maxMessages >= 0)
        stream.slice(0, maxMessages)
      else
        stream

    val formatter: MessageFormatter = messageFormatterClass.newInstance().asInstanceOf[MessageFormatter]
    formatter.init(formatterArgs)

    try {
      for(message <- iter) {
        try {
          formatter.writeTo(message, System.out)
        } catch {
          case e :Throwable =>
            if (skipMessageOnError)
              logger.error("error processing message, skipping and resume consumption: " + e)
            else
              throw e
        }
      }
    } catch {
      case  e : Throwable => logger.error("error processing message, stop consuming: " + e)
    }
      
    System.out.flush()
    formatter.close()
    connector.shutdown()
  }
  
  def tryParse(parser: OptionParser, args: Array[String]) = {
    try {
      parser.parse(args : _*)
    } catch {
      case e: OptionException => {
        Utils.croak(e.getMessage)
        null
      }
    }
  }
  
  def checkRequiredArgs(parser: OptionParser, options: OptionSet, required: OptionSpec[_]*) {
    for(arg <- required) {
    	if(!options.has(arg)) {
        logger.error("Missing required argument \"" + arg + "\"")
    		parser.printHelpOn(System.err)
        System.exit(1)
      }
    }
  }

  def tryParseFormatterArgs(args: Iterable[String]): Properties = {
        val splits = args.map(_ split "=").filterNot(_ == null).filterNot(_.length == 0)
        if(!splits.forall(_.length == 2)) {
          System.err.println("Invalid parser arguments: " + args.mkString(" "))
          System.exit(1)
        }
        val props = new Properties
        for(a <- splits)
          props.put(a(0), a(1))
        props

  }

  trait MessageFormatter {
    def writeTo(message: Message, output: PrintStream)
    def init(props: Properties) {}
    def close() {}
  }
  
  class NewlineMessageFormatter extends MessageFormatter {
    def writeTo(message: Message, output: PrintStream) {
      val payload = message.payload
      output.write(payload.array, payload.arrayOffset, payload.limit)
      output.write('\n')
    }
  }
  
  def tryCleanupZookeeper(zkUrl: String, groupId: String) {
    try {
      val dir = "/consumers/" + groupId
      logger.info("Cleaning up temporary zookeeper data under " + dir + ".")
      val zk = new ZkClient(zkUrl, 30*1000, 30*1000, StringSerializer)
      zk.deleteRecursive(dir)
      zk.close()
    } catch {
      case   _ : Throwable => // swallow
    }
  }
   
}
