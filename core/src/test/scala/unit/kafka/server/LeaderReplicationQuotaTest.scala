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

package unit.kafka.server

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.common._
import kafka.server.{TempThrottleTypes, ClientConfigOverride, KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.serialization.{BytesSerializer, StringSerializer}
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable

class LeaderReplicationQuotaTest extends ZooKeeperTestHarness {
  val ERROR: Int = 500
  var brokers: Seq[KafkaServer] = null
  var leader: KafkaServer = null
  var follower: KafkaServer = null
  val topic1 = "foo"
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null
  var leaderMetricName: MetricName = null
  var followerMetricName: MetricName = null

  @Before
  override def setUp() {
    super.setUp()
    //Create two brokers with one partition then figure out who the leader is.
    brokers = createBrokerConfigs(2, zkConnect)
      .map(KafkaConfig.fromProps)
      .map(TestUtils.createServer(_))
    val leaders = TestUtils.createTopic(zkUtils,
      topic1,
      numPartitions = 1,
      replicationFactor = 2,
      servers = brokers)
    leader = if (leaders(0).get == brokers.head.config.brokerId) brokers.head else brokers(1)
    follower = if (leaders(0).get == brokers.head.config.brokerId) brokers(1) else brokers.head
    producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(brokers), retries = 5)
    leaderMetricName = leader.metrics.metricName("throttle-time",
      ApiKeys.FETCH.name,
      "Tracking throttle-time per client",
      "client-id", TempThrottleTypes.leaderThrottleKey)
    followerMetricName = follower.metrics.metricName("throttle-time",
      "apikey.replication",
      "Tracking throttle-time per client",
      "client-id", TempThrottleTypes.followerThrottleKey)
  }

  @After
  override def tearDown() {
    brokers.foreach(_.shutdown())
    super.tearDown()
  }

  @Test
  def testQuotaInvokesExpectedDelayOnSingleMessage() {
    val props = new Properties()
    val throttle: Int = 50 * 1000
    val msg: Array[Byte] = new Array[Byte]( 800 * 1000) //~800K

    //Given
    props.put(ClientConfigOverride.ConsumerOverride, throttle.toString)
    AdminUtils.changeClientIdConfig(zkUtils, TempThrottleTypes.leaderThrottleKey, props)
    val start = System.currentTimeMillis()

    producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(brokers),
      retries = 5)
    //When
    val record: ProducerRecord[Array[Byte], Array[Byte]] = new ProducerRecord(topic1, msg)
    producer.send(record).get
    producer.close()

    //Then we should get the correct delay imposed by the quota
    //throttle = 50K x 10  = 500K over 10s window. delta =  800K - 500K = 300K
    //so we expect a delay of 300K/500K x 10 = 30/50 * 10 = 6s
    val throttledTime: Double = leader.metrics.metrics.asScala(leaderMetricName).value()
    assertEquals("Throttle time should be 6s", throttledTime, 6000, ERROR)

    //Then replication should complete
    waitUntilTrue(logsMatch, "Broker logs should be identical", 30000)

    println("Took: " + (System.currentTimeMillis() - start) + " and should have taken: " + msg.length / throttle)
  }

  @Test //probably too long and too fragile to check in
  def testQuotaProducesDesiredRateOverTime() {
    val props = new Properties()
    val throttle: Int = 100 * 1000
    val msg: Array[Byte] = new Array[Byte](1000) //~1k with overhead
    val numMessages = 4000

    //Given
    props.put(ClientConfigOverride.ConsumerOverride, throttle.toString)
    AdminUtils.changeClientIdConfig(zkUtils, TempThrottleTypes.leaderThrottleKey, props)
    val start = System.currentTimeMillis()

    //When
    for (x <- 0 to numMessages) {
      val record: ProducerRecord[Array[Byte], Array[Byte]] = new ProducerRecord(topic1, msg)
      producer.send(record).get
      println("sent message " + x)
    }
    producer.close()
    waitUntilTrue(logsMatch, "Broker logs should be identical", 30000)

    //Then replication should take as long as expected
    val expectedDuration: Int = msg.length * numMessages / throttle * 1000
    val took: Long = System.currentTimeMillis() - start
    val desc = "Took: " + took + "ms and should have taken: " + expectedDuration
    println(desc)
    assertEquals(desc, expectedDuration, took, expectedDuration * 0.2)
  }

  def logsMatch(): Boolean = {
    var result = true
    val topicAndPart = TopicAndPartition(topic1, 0)
    val expectedOffset = brokers.head.getLogManager().getLog(topicAndPart).get.logEndOffset
    result = result && expectedOffset > 0 && brokers.forall { item =>
      expectedOffset == item.getLogManager().getLog(topicAndPart).get.logEndOffset
    }
    if (result) println("final offset was " + expectedOffset)
    result
  }

  @Test
  def testFollowerQuotaInvokesExpectedDelayOnSingleMessage() {
    val props = new Properties()
    val throttle: Int = 50 * 1000
    val msg: Array[Byte] = new Array[Byte]( 800 * 1000) //~800K

    //Given
    props.put(ClientConfigOverride.ConsumerOverride, throttle.toString)
    AdminUtils.changeClientIdConfig(zkUtils, TempThrottleTypes.followerThrottleKey, props)
    val start = System.currentTimeMillis()

    producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(brokers), retries = 5)
    //When
    val record: ProducerRecord[Array[Byte], Array[Byte]] = new ProducerRecord(topic1, msg)
    producer.send(record).get
    producer.close()

    //Then we should get the correct delay imposed by the quota
    //throttle = 50K x 10  = 500K over 10s window. delta =  800K - 500K = 300K
    //so we expect a delay of 300K/500K x 10 = 30/50 * 10 = 6s
    val throttledTime: Double = follower.metrics.metrics().asScala(followerMetricName).value()
    assertEquals("Throttle time should be 6s", throttledTime, 6000, ERROR)

    //Ensure leader throttle did not enable
    val throttledTimeLeader: Double = leader.metrics.metrics().asScala(leaderMetricName).value()
    assertEquals("Throttle on leader should not be engaged", throttledTimeLeader, 0, 0)

    //Then replication should complete
    waitUntilTrue(logsMatch, "Broker logs should be identical", 30000)

    println("Took: " + (System.currentTimeMillis() - start) + " and should have taken: " + msg.length / throttle)
  }

}
