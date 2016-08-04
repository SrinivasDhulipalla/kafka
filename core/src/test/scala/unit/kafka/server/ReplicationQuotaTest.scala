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
import kafka.server.ClientConfigOverride._
import kafka.server.KafkaConfig._
import kafka.server.TempThrottleTypes._
import kafka.server._
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

class ReplicationQuotaTest extends ZooKeeperTestHarness {
  val ERROR: Int = 500
  val msg1KB = new Array[Byte](1000)
  val msg800KB = new Array[Byte](800 * 1000)
  var brokers: Seq[KafkaServer] = null
  var leader: KafkaServer = null
  var follower: KafkaServer = null
  val topic1 = "topic1"
  val topic2 = "topic2"
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null
  var leaderMetricName: MetricName = null
  var followerMetricName: MetricName = null
  val replicaProps = new Properties()
  val replicasProps = new Properties()

  @Before
  override def setUp() {
    super.setUp()
    //Create two brokers with one partition then figure out who the leader is.
    brokers = createBrokerConfigs(2, zkConnect)
      .map(fromProps)
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
      "client-id", leaderThrottleKey)
    followerMetricName = follower.metrics.metricName("throttle-time",
      "apikey.replication",
      "Tracking throttle-time per client",
      "client-id", followerThrottleKey)
    replicaProps.clear()
    replicasProps.clear()
  }

  @After
  override def tearDown() {
    brokers.foreach(_.shutdown())
    producer.close()
    super.tearDown()
  }

  //TODO we need to test with multiple brokers.
  //TODO - can probably ditch TempThrottleTypes having two types now.
  //TODO - some configuratios lead to the "delay time" imposed by the quota manager being negative (and significant values). Why?


  @Test
  def shouldThrottleSingleMessageOnLeader() {

    //Given only one throttled replica, on the leader
    val throttle: Int = 50 * 1000
    replicaProps.put(ConsumerOverride, throttle.toString)
    AdminUtils.changeClientIdConfig(zkUtils, leaderThrottleKey, replicaProps)

    replicasProps.put(ReplicationQuotaThrottledReplicas, "0-" + leader.config.brokerId)
    AdminUtils.changeTopicConfig(zkUtils, topic1, replicasProps)

    //When
    producer.send(new ProducerRecord(topic1, msg800KB)).get
    waitUntilTrue(logsMatch, "Broker logs should be identical", 30000)

    //Then the correct delay should be imposed by the quota
    //throttle = 50K x 10s  = 500KB over 10s window. delta =  800K - 500K = 300K
    //so we expect a delay of 300K/(50K x10) x 10 = 300K/50K = 6s
    val expectedDuration = (msg800KB.length - throttle * 10) / throttle * 1000
    val throttledTime: Double = leader.metrics.metrics.asScala(leaderMetricName).value()
    assertEquals("Throttle time should be " + expectedDuration, expectedDuration, throttledTime, ERROR)

    //Ensure follower throttle did not enable
    assertFalse(follower.metrics.metrics().containsKey(followerMetricName))
  }

  @Test
  def shouldThrottleSingleMessageOnFollower() {

    //Given only one throttled replica, on the follower
    val throttle: Int = 50 * 1000
    replicaProps.put(ConsumerOverride, throttle.toString)
    AdminUtils.changeClientIdConfig(zkUtils, followerThrottleKey, replicaProps)

    replicasProps.put(ReplicationQuotaThrottledReplicas, "0-" + follower.config.brokerId)
    AdminUtils.changeTopicConfig(zkUtils, topic1, replicasProps)

    val start = System.currentTimeMillis()

    //When
    producer.send(new ProducerRecord(topic1, msg800KB)).get
    waitUntilTrue(logsMatch, "Broker logs should be identical", 30000)

    //Then the correct delay should be imposed by the quota
    //throttle = 50K x 10s  = 500KB over 10s window. delta =  800K - 500K = 300K
    //so we expect a delay of 300K/(50K x10) x 10 = 300K/50K = 6s
    val expectedDuration = (msg800KB.length - throttle * 10) / throttle * 1000
    val throttledTime: Double = follower.metrics.metrics().asScala(followerMetricName).value()
    assertEquals("Throttle time should be "+expectedDuration,expectedDuration, throttledTime, ERROR)

    //Ensure leader throttle did not enable
    assertFalse(leader.metrics.metrics().containsKey(leaderMetricName))

  }

  @Test //probably too long and too fragile to check in. gets more accurate the more messages/lower-quota (i.e. longer running)
  def shouldThrottleToDesiredRateOnLeaderOverTime() {

    //Given
    val throttle: Int = 300 * 1000
    replicaProps.put(ConsumerOverride, throttle.toString)
    AdminUtils.changeClientIdConfig(zkUtils, leaderThrottleKey, replicaProps)

    replicasProps.put(ReplicationQuotaThrottledReplicas, "0-" + leader.config.brokerId)
    AdminUtils.changeTopicConfig(zkUtils, topic1, replicasProps)

    val start = System.currentTimeMillis()

    //When
    for (x <- 0 to 8000)
      producer.send(new ProducerRecord(topic1, msg1KB)).get
    waitUntilTrue(logsMatch, "Broker logs should be identical", 30000)

    //Then replication should take as long as expected
    val expectedDuration = msg1KB.length * 8000 / throttle * 1000
    val took: Long = System.currentTimeMillis() - start
    val desc = "Took: " + took + "ms and should have taken: " + expectedDuration
    println(desc)
    assertEquals(desc, expectedDuration, took, expectedDuration * 0.2)
  }

  @Test //probably too long and too fragile to keep enabled
  def shouldThrottleToDesiredRateOnFollowerOverTime() {

    //Given
    val throttle: Int = 300 * 1000
    replicaProps.put(ConsumerOverride, throttle.toString)
    AdminUtils.changeClientIdConfig(zkUtils, followerThrottleKey, replicaProps)

    replicasProps.put(ReplicationQuotaThrottledReplicas, "0-" + follower.config.brokerId)
    AdminUtils.changeTopicConfig(zkUtils, topic1, replicasProps)

    val start = System.currentTimeMillis()

    //When
    for (x <- 0 to 8000)
      producer.send(new ProducerRecord(topic1, msg1KB)).get
    waitUntilTrue(logsMatch, "Broker logs should be identical", 30000)

    //Then replication should take as long as expected
    val expectedDuration = msg1KB.length * 8000 / throttle * 1000
    val took = System.currentTimeMillis() - start
    assertEquals("Took: " + took + "ms but should have taken: " + expectedDuration,
      expectedDuration, took, expectedDuration * 0.2)
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


}
