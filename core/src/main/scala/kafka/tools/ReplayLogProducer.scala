/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicLong

import joptsimple.OptionParser
import kafka.consumer._
import kafka.utils.{CommandLineUtils, Logging, ToolsUtils}
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import scala.collection.JavaConversions._


trait Configuration {
  val numMessages: Int
  val numThreads: Int
  val inputTopic: String
  val outputTopic: String
  val isSync: Boolean
  val reportingInterval: Int
  val producerProps: Properties
  val consumerProps: Properties
}

class ReplayLogProducer extends Logging {
  var latch: CountDownLatch = _
  var threadList = List[ReplayThread]()
  val globalCount = new AtomicLong()

  def main(args: Array[String]) {
    new ReplayLogProducer()
      .run(new ConfigBuilder(args))
      .awaitTerminationAndClose()
  }

  def run(config: Configuration): ReplayLogProducer = {
    run(config, new NoOpConsumerRebalanceListener())
  }

  def run(config: Configuration, rebalanceListener: ConsumerRebalanceListener): ReplayLogProducer = {
    latch = new CountDownLatch(config.numThreads)

    for (a <- 1 to config.numThreads) {
      threadList ::= new ReplayThread(config, latch, globalCount, rebalanceListener)
    }
    for (thread <- threadList)
      thread.start
    this
  }

  def awaitTerminationAndClose(): Unit = {
    latch.await
    threadList.foreach(_.shutdown)
  }

  class ReplayThread(config: Configuration, latch: CountDownLatch, globalCounter: AtomicLong, rebalanceListener: ConsumerRebalanceListener) extends Thread with Logging {
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](config.producerProps)
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config.consumerProps)

    override def run() {
      consumer.subscribe(List(config.inputTopic), rebalanceListener)
      var localCounter = 0
      while (!done) {
        info("Thread %s for partitions %s completed poll returning %s records at global msg counter %s".format(Thread.currentThread.getId, consumer.assignment(), consumer.subscription(), globalCounter.get))

        for (record <- consumer.poll(100)) {
          if (!done) {
            try {
              val response = producer.send(new ProducerRecord[Array[Byte], Array[Byte]](config.outputTopic, record.key(), record.value()))
              if (config.isSync)
                response.get()
            } catch {
              case ie: Exception => error("Skipping this message", ie)
            }
            globalCounter.incrementAndGet()
            localCounter += 1
          }
          if (localCounter % config.reportingInterval == 0)
            info("Thread %s for partitions %s completed poll at global msg counter %s".format(Thread.currentThread.getId, consumer.assignment(), globalCounter.get))
        }
      }
      latch.countDown
      info("Thread %s completed processing having sent %s messages".format(Thread.currentThread().getId, localCounter))
    }

    def done: Boolean = {
      globalCounter.get >= config.numMessages
    }

    def shutdown() {
      producer.close
      consumer.close
    }

    override def info(msg: => String): Unit = {
      println(msg)
    }
  }


  class ConfigBuilder(args: Array[String]) extends Configuration {

    val parser = new OptionParser
    val brokerListOpt = parser.accepts("broker-list", "REQUIRED: the broker list must be specified.")
      .withRequiredArg
      .describedAs("hostname:port")
      .ofType(classOf[String])
    val inputTopicOpt = parser.accepts("input-topic", "REQUIRED: The topic to consume from.")
      .withRequiredArg
      .describedAs("input-topic")
      .ofType(classOf[String])
    val outputTopicOpt = parser.accepts("output-topic", "REQUIRED: The topic to produce to")
      .withRequiredArg
      .describedAs("output-topic")
      .ofType(classOf[String])
    val numMessagesOpt = parser.accepts("messages", "The number of messages to send.")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(-1)
    val numThreadsOpt = parser.accepts("threads", "Number of sending threads.")
      .withRequiredArg
      .describedAs("threads")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)
    val reportingIntervalOpt = parser.accepts("reporting-interval", "Interval at which to print progress info.")
      .withRequiredArg
      .describedAs("size")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(0)
    val producerPropsOpt = parser.accepts("producer-properties", "A mechanism to pass properties in the form key=value to the producer. " +
      "This allows the user to override producer properties that are not exposed by the existing command line arguments")
      .withRequiredArg
      .describedAs("producer properties")
      .ofType(classOf[String])
    val consumerPropsOpt = parser.accepts("consumer-properties", "A mechanism to pass properties in the form key=value to the consumer. " +
      "This allows the user to override consumer properties that are not exposed by the existing command line arguments")
      .withRequiredArg
      .describedAs("consumer properties")
      .ofType(classOf[String])
    val genericProsOpt = parser.accepts("generic-properties", "A mechanism to pass properties in the form key=value to both the producer and the consumer. " +
      "This allows the user to override producer and consumer properties that are not exposed by the existing command line arguments")
      .withRequiredArg
      .describedAs("generic properties")
      .ofType(classOf[String])

    val syncOpt = parser.accepts("sync", "If set message send requests to the brokers are synchronously, one at a time as they arrive.")

    val options = parser.parse(args: _*)

    CommandLineUtils.checkRequiredArgs(parser, options, brokerListOpt, inputTopicOpt)

    val brokerList = options.valueOf(brokerListOpt)
    ToolsUtils.validatePortOrDie(parser, brokerList)
    val numMessages = options.valueOf(numMessagesOpt).intValue
    val numThreads = options.valueOf(numThreadsOpt).intValue
    val inputTopic = options.valueOf(inputTopicOpt)
    val outputTopic = options.valueOf(outputTopicOpt)
    val reportingInterval = options.valueOf(reportingIntervalOpt).intValue
    val isSync = options.has(syncOpt)

    val genericProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(genericProsOpt))

    val producerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(producerPropsOpt))
    producerProps.putAll(genericProps)
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

    val consumerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(consumerPropsOpt))
    consumerProps.putAll(genericProps)
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis())
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "0") //ensure we have no temporal batching
  }

}
