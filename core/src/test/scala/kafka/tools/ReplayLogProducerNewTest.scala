/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import java.util
import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit._

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.SecurityProtocol
import org.junit.{After, Assert, Test}

import scala.collection.JavaConversions._

class ReplayLogProducerNewTest extends KafkaServerTestHarness {

  @Test
  def shouldPushMessagesFromInboundToOutboundTopic(): Unit = {
    val config = new kafka.tools.Configuration {
      override val numMessages: Int = 10
      override val consumerProps: Properties = consumerProperties
      override val producerProps: Properties = producerProperties
      override val isSync: Boolean = false
      override val outputTopic: String = "out"
      override val inputTopic: String = "in"
      override val numThreads: Int = 1
      override val reportingInterval: Int = 1
    }
    val dataGenerator = new KafkaProducer[Array[Byte], Array[Byte]](config.producerProps)
    val consumerPartitionAllocated = new CountDownLatch(1)

    //Given
    val replayer = new ReplayLogProducerNew().run(config, evenBalanceListener(consumerPartitionAllocated))

    consumerPartitionAllocated.await()

    //When
    for (range <- 1 to config.numMessages) {
      dataGenerator.send(record(config.inputTopic)).get()
    }
    dataGenerator.flush()

    replayer.awaitTerminationAndClose()

    //Then
    Assert.assertEquals(config.numMessages, countRecords(config.outputTopic))
  }

  @Test
  def shouldPushMessagesUsingMultipleThreads(): Unit = {

    val config = new kafka.tools.Configuration {
      override val numMessages: Int = 500
      override val consumerProps: Properties = consumerProperties
      override val producerProps: Properties = producerProperties
      override val isSync: Boolean = false
      override val outputTopic: String = "out"
      override val inputTopic: String = "in"
      override val numThreads: Int = 11
      override val reportingInterval: Int = 1
    }
    val dataGenerator = new KafkaProducer[Array[Byte], Array[Byte]](config.producerProps)
    val evenBalance = new CountDownLatch(config.numThreads)

    //Create topic with one partition per thread
    TestUtils.createTopic(zkClient, config.inputTopic, config.numThreads, 1, servers, new Properties())

    //Start the log replayer
    val replayer = new ReplayLogProducerNew().run(config, evenBalanceListener(evenBalance))

    //Wait for each consumer (thread) to be assigned a single partition
    evenBalance.await(15, SECONDS)
    if (evenBalance.getCount > 0)
      fail("Consumers did not reach even balance within timeout")

    //Populate the inbound topic

    for (range <- 1 to config.numMessages)
      dataGenerator.send(record(config.inputTopic)).get()
    dataGenerator.flush()

    //Await completion
    replayer.awaitTerminationAndClose()

    //Ensure messages were copied to outbound topic
    Assert.assertEquals(config.numMessages, countRecords(config.outputTopic))
  }


  @Test
  def shouldLimitByNumberMessagesParameter(): Unit = {
    val config = new kafka.tools.Configuration {
      override val numMessages: Int = 10
      override val consumerProps: Properties = consumerProperties
      override val producerProps: Properties = producerProperties
      override val isSync: Boolean = false
      override val outputTopic: String = "out"
      override val inputTopic: String = "in"
      override val numThreads: Int = 1
      override val reportingInterval: Int = 1
    }
    val dataGenerator = new KafkaProducer[Array[Byte], Array[Byte]](config.producerProps)
    val consumerPartitionAllocated = new CountDownLatch(1)

    //Given
    val replayer = new ReplayLogProducerNew().run(config, evenBalanceListener(consumerPartitionAllocated))

    consumerPartitionAllocated.await()

    //When
    for (range <- 1 to config.numMessages + 10) {
      dataGenerator.send(record(config.inputTopic)).get()
    }
    dataGenerator.flush()

    replayer.awaitTerminationAndClose()

    //Then
    Assert.assertEquals(config.numMessages, countRecords(config.outputTopic))
  }

  def evenBalanceListener(evenBalance: CountDownLatch): ConsumerRebalanceListener = {
    new ConsumerRebalanceListener() {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        if (partitions.size() == 1)
          evenBalance.countDown()
      }

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {}
    }
  }

  def countRecords(topic: String): Int = {
    val consumer = consumerInDifferentGroup(topic)
    val timeout: Long = System.currentTimeMillis() + 3000
    var count = 0
    while (System.currentTimeMillis() < timeout) {
      val poll: ConsumerRecords[Array[Byte], Array[Byte]] = consumer.poll(100)
      for (r <- poll) {
        count += 1
      }
    }
    count
  }

  def consumerInDifferentGroup(topic: String): KafkaConsumer[Array[Byte], Array[Byte]] = {
    val props = new Properties()
    props.putAll(consumerProperties)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "testObserver" + System.currentTimeMillis())
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
    consumer.subscribe(List(topic))
    consumer
  }

  def record(topic: String): ProducerRecord[Array[Byte], Array[Byte]] = {
    new ProducerRecord[Array[Byte], Array[Byte]](topic, "value".getBytes())
  }

  def consumerProperties: Properties = {
    val propConsumer: Properties = new Properties()
    propConsumer.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + servers(1).boundPort(SecurityProtocol.PLAINTEXT))
    propConsumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    propConsumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    propConsumer.put(ConsumerConfig.GROUP_ID_CONFIG, "replay-log-producer-shared-group")
    //        propConsumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    propConsumer
  }

  def producerProperties: Properties = {
    val propProducer: Properties = new Properties()
    propProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + servers(1).boundPort(SecurityProtocol.PLAINTEXT))
    propProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    propProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    propProducer
  }

  @After
  override def tearDown() {
    super.tearDown()
  }

  override def generateConfigs(): Seq[KafkaConfig] = TestUtils.createBrokerConfigs(2, zkConnect, false).map(KafkaConfig.fromProps(_, new Properties()))
}