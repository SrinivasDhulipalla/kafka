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

package unit.kafka.integration

import java.util.Properties
import java.{util => ju, lang => jl}
import java.util.concurrent.{TimeUnit, CountDownLatch}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import scala.collection.JavaConversions._
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils._
import kafka.utils.{Logging, CoreUtils, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRebalanceListener, ConsumerRecords, ConsumerConfig}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.protocol.SecurityProtocol
import org.hamcrest.core.Is
import org.hamcrest.core.Is._
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.mutable.ListBuffer

class ProducerFailoverUnderServerDeathTest extends ZooKeeperTestHarness {


  val partitionId = 0

  var configs: Seq[Properties] = Seq.empty[Properties]
  var servers: Seq[KafkaServer] = null

  //  var servers: ListBuffer[KafkaServer] = new ListBuffer[KafkaServer]

  @Before
  override def setUp() {

    CoreUtils.rm("controller.log")
    CoreUtils.rm("server.log")
    CoreUtils.rm("state-change.log")
    println("should have gone")
    Thread.sleep(3000)

    super.setUp()
    // controlled.shutdown.enable is true by default
    configs = (0 until 4).map(i => TestUtils.createBrokerConfig(i, zkConnect))
    configs(3).put("controlled.shutdown.retry.backoff.ms", "100")

    // start all the servers
    servers = configs.map(c => TestUtils.createServer(KafkaConfig.fromProps(c)))

    //    servers += TestUtils.createServer(KafkaConfig.fromProps(configs.get(0)))
    //    servers += (TestUtils.createServer(KafkaConfig.fromProps(configs.get(1))))
    //    servers += (TestUtils.createServer(KafkaConfig.fromProps(configs.get(2))))

  }

  @After
  override def tearDown() {
    servers.foreach(_.shutdown())
    servers.foreach(server => CoreUtils.rm(server.config.logDirs))
    super.tearDown()
  }

  @Test
  def shouldGoHorriblyWrong {
      // start all the brokers
      val topic1 = "new-topic1"

      // create topics with 1 partition, 2 replicas, one on each broker
      createTopic(zkUtils, topic1, 10, 3, servers = servers)

      val dataGenerator: ProducingThread = new ProducingThread(producerProperties, 1000, topic1)

      dataGenerator.start()
      Thread.sleep(5000)
      assertTrue(dataGenerator.sent.get > 0)
      assertTrue(dataGenerator.acked.get > 0)

      //    servers(0).shutdown()
      println("bouncing first")
      bounceServer(topic1, 0)
      println("done bouncing first")

      Thread.sleep(5000)
      println("bouncing second")
      bounceServer(topic1, 1)
      println("done second")

      dataGenerator.end()

      assertThat(countRecords(topic1), is(dataGenerator.sent.get.toInt))
      assertTrue(dataGenerator.sent.get > 0)

      println("\n\n\nDone " + dataGenerator.sent.get + " \n\n\n")

  }



  private def bounceServer(topic: String, startIndex: Int) {
    var prevLeader = 0
    if (isLeaderLocalOnBroker(topic, partitionId, servers(startIndex))) {
      servers(startIndex).hackdown()
      prevLeader = startIndex
    }
    else {
      servers((startIndex + 1) % 4).shutdown()
      prevLeader = (startIndex + 1) % 4
    }
    val newleader = waitUntilLeaderIsElectedOrChanged(zkUtils, topic, partitionId, timeoutMs = 20000)
    // Ensure the new leader is different from the old
    assertTrue("Leader transition did not happen for " + topic, newleader.getOrElse(-1) != -1 && (newleader.getOrElse(-1) != prevLeader))
    // Start the server back up again
    servers(prevLeader).startup()
  }

  def producerProperties: Properties = {
    val propProducer: Properties = new Properties()
    propProducer.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + servers(1).boundPort(SecurityProtocol.PLAINTEXT))
    propProducer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    propProducer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    propProducer
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


  def countRecords(topic: String): Int = {
    val consumer = consumerInDifferentGroup(topic)
    val timeout: Long = System.currentTimeMillis() + 3000
    var count = 0
    while (System.currentTimeMillis() < timeout) {
      val poll: ConsumerRecords[Array[Byte], Array[Byte]] = consumer.poll(100)
      for (r <- poll) {
        count += 1
      }
      println("looking for stuff")
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


  class ProducingThread(producerProperties: Properties, count: Int, topic: String) extends Thread with Logging {

    var acked = new AtomicLong()
    var sent = new AtomicLong()
    var running = new AtomicBoolean(true)
    var broken = new AtomicBoolean(false)

    override def run() {
      val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProperties)
      try {
        while (running.get) {
          val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, s"key$sent".getBytes(), s"value$sent".getBytes())
          val response = producer.send(record, callback).get()
          debug(response.toString)
          sent.incrementAndGet()
        }

      } catch {
        case e: Exception =>
          e.printStackTrace
          error("All bests are off")
          broken.set(true)
      }
      producer.flush()
    }

    def end(): Unit = {
      running.set(false)
      val start: Long = System.currentTimeMillis()
      val timeout: Int = 10 * 1000
      while (System.currentTimeMillis() < start + timeout && acked.get < sent.get)
        Thread.sleep(100)

      if (sent.get() < acked.get())
        throw new RuntimeException(s"some messages were not acknowledged sent: ${sent.get} acked: ${acked.get} wereExceptions: ${broken.get()}")
    }

    def callback: Callback with Object {def onCompletion(metadata: RecordMetadata, exception: Exception): Unit} = {
      new Callback() {
        var lastCall = 0L
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (metadata == null) {
            info("Fuck")
            exception.printStackTrace()
          }
          else {
            //            info("acked message " + metadata)
            acked.incrementAndGet()
            val since: Long = System.currentTimeMillis() - lastCall
            if(lastCall>0 &&  since > 300)
              println(s"\n\n\n\n\n\n\n*****\n no call for $since \n\n\n\n\n\n")
            lastCall = System.currentTimeMillis
          }

        }
      }
    }
  }

}