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
import kafka.server.QuotaFactory.QuotaType
import kafka.server.QuotaFactory.QuotaType._
import kafka.server._
import kafka.utils.{CoreUtils, TestUtils}
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.{KafkaException, MetricName}
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

class ReplicationQuotaTest extends ZooKeeperTestHarness {
  val ERROR: Int = 1000
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
  var leaderDelayQueueMetricName: MetricName = null
  val props1 = new Properties()
  val props2 = new Properties()

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
      LeaderReplication.toString,
      "Tracking throttle-time per client",
      "client-id", LeaderReplication.toString)
    followerMetricName = follower.metrics.metricName("throttle-time",
      FollowerReplication.toString,
      "Tracking throttle-time per client",
      "client-id", FollowerReplication.toString)
    leaderDelayQueueMetricName = leader.metrics.metricName("queue-size",
      QuotaType.LeaderReplication.toString,
      "Tracks the size of the delay queue")
    props1.clear()
    props2.clear()
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
    props1.put(ConsumerOverride, throttle.toString)
    AdminUtils.changeClientIdConfig(zkUtils, LeaderReplication.toString, props1)

    props2.put(ReplicationQuotaThrottledReplicas, "0-" + leader.config.brokerId)
    AdminUtils.changeTopicConfig(zkUtils, topic1, props2)

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
    props1.put(ConsumerOverride, throttle.toString)
    AdminUtils.changeClientIdConfig(zkUtils, FollowerReplication.toString, props1)

    props2.put(ReplicationQuotaThrottledReplicas, "0-" + follower.config.brokerId)
    AdminUtils.changeTopicConfig(zkUtils, topic1, props2)

    //When
    producer.send(new ProducerRecord(topic1, msg800KB)).get
    waitUntilTrue(logsMatch, "Broker logs should be identical", 30000)

    //Then the correct delay should be imposed by the quota
    //throttle = 50K x 10s  = 500KB over 10s window. delta =  800K - 500K = 300K
    //so we expect a delay of 300K/(50K x10) x 10 = 300K/50K = 6s
    val expectedDuration = (msg800KB.length - throttle * 10) / throttle * 1000
    val throttledTime: Double = follower.metrics.metrics().asScala(followerMetricName).value()
    assertEquals("Throttle time should be " + expectedDuration, expectedDuration, throttledTime, ERROR)

    //Ensure leader throttle did not enable
    assertFalse(leader.metrics.metrics().containsKey(leaderMetricName))
  }

  @Test //probably too long and too fragile to check in. gets more accurate the more messages/lower-quota (i.e. longer running)
  def shouldThrottleToDesiredRateOnLeaderOverTime() {

    //Given
    val throttle: Int = 300 * 1000
    props1.put(ConsumerOverride, throttle.toString)
    AdminUtils.changeClientIdConfig(zkUtils, LeaderReplication.toString, props1)

    props2.put(ReplicationQuotaThrottledReplicas, "0-" + leader.config.brokerId)
    AdminUtils.changeTopicConfig(zkUtils, topic1, props2)

    val start = System.currentTimeMillis()

    //When
    for (x <- 0 to 8000)
      producer.send(new ProducerRecord(topic1, msg1KB)).get
    waitUntilTrue(logsMatch, "Broker logs should be identical", 30000)

    //Then replication should take as long as expected
    val expectedDuration = msg1KB.length * 8000 / throttle * 1000
    val took: Long = System.currentTimeMillis() - start
    val desc = "Took: " + took + "ms and should have taken: " + expectedDuration
    info(desc)
    assertEquals(desc, expectedDuration, took, expectedDuration * 0.2)
  }

  @Test //probably too long and too fragile to keep enabled
  def shouldThrottleToDesiredRateOnFollowerOverTime() {

    //Given
    val throttle: Int = 300 * 1000
    props1.put(ConsumerOverride, throttle.toString)
    AdminUtils.changeClientIdConfig(zkUtils, FollowerReplication.toString, props1)

    props2.put(ReplicationQuotaThrottledReplicas, "0-" + follower.config.brokerId)
    AdminUtils.changeTopicConfig(zkUtils, topic1, props2)

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

  @Test
  def shouldNotReorderDuringReplicaHandoverBetweenThreadsOnFollower() {


    val producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(brokers), retries = 5, acks = 1)
    val topic = "single-partition-topic"
    val leader = brokers(0)
    val follower = brokers(1)
    TestUtils.createTopic(zkUtils, topic, Map(0 -> Seq(0, 1)), brokers)


    //Leader throttle to produce 6s delay
    val throttle = 50 * 1000
    props1.put(ConsumerOverride, throttle.toString)
    AdminUtils.changeClientIdConfig(zkUtils, LeaderReplication.toString, props1)

    props2.put(ReplicationQuotaThrottledReplicas, "0-" + leader.config.brokerId)
    AdminUtils.changeTopicConfig(zkUtils, topic, props2)

    //Send a message
    producer.send(new ProducerRecord(topic, 0, null, msg800KB)).get
    def done(): Boolean = leader.metrics.metrics.asScala(leaderDelayQueueMetricName).value == 1

    waitUntilTrue(done, "leader should be throttled")

    //The leader should now be blocking replication.

    //Add a follower throttle, this should create a second request from the other threadpool
    props1.put(ConsumerOverride, throttle.toString)
    AdminUtils.changeClientIdConfig(zkUtils, FollowerReplication.toString, props1)

    props2.put(ReplicationQuotaThrottledReplicas, "0-" + follower.config.brokerId)
    AdminUtils.changeTopicConfig(zkUtils, topic, props2)


    waitUntilTrue(logsMatch, "Broker logs should be identical", 30000)
  }

  //


  def waitForConfigToPropagate(topic: String): Boolean = {
    def configPropagated(): Boolean = {
      brokers(1).quotaManagers(FollowerReplication).throttledReplicas.isThrottled(new TopicAndPartition(topic, 0)) && brokers(1).quotaManagers(FollowerReplication).overriddenQuota.containsKey(FollowerReplication.toString)
    }
    waitUntilTrue(configPropagated, "")
  }

  def logsMatch(): Boolean = logsMatch(TopicAndPartition(topic1, 0))

  def logsMatch(topicAndPart: TopicAndPartition): Boolean = {
    var result = true
    val expectedOffset = brokers.head.getLogManager().getLog(topicAndPart).get.logEndOffset
    result = result && expectedOffset > 0 && brokers.forall { item =>
      expectedOffset == item.getLogManager().getLog(topicAndPart).get.logEndOffset
    }
    if (result) info("final offset was " + expectedOffset + " for partition " + topicAndPart)
    result
  }

  def waitForOffset(topicAndPart: TopicAndPartition, offset: Int): Boolean = {
    var result = true
    result = result && brokers.forall { item =>
      offset == item.getLogManager().getLog(topicAndPart).get.logEndOffset
    }
    if (result) info("final offset was " + offset + " for partition " + topicAndPart)
    result
  }
}
