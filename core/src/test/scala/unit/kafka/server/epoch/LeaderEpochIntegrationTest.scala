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
package unit.kafka.server.epoch

import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils._
import kafka.utils.{Logging, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.{After, Before, Test}

class LeaderEpochIntegrationTest extends ZooKeeperTestHarness  with Logging{
  var brokers: Seq[KafkaServer] = null
  val topic1 = "foo"
  val topic2 = "bar"

  @Before
  override def setUp() {
    super.setUp()
    val props = createBrokerConfigs(2, zkConnect)
    brokers = props.map(KafkaConfig.fromProps).map(TestUtils.createServer(_))
  }

  @After
  override def tearDown() {
    brokers.foreach(_.shutdown())
    super.tearDown()
  }

  @Test
  def shouldAddCurrentLeaderEpochToMessagesAsTheyAreWrittenToBroker() {
    // Given two topics with replication of a single partition
    for (topic <- List(topic1, topic2)) {
      createTopic(zkUtils, topic, Map(0 -> Seq(0, 1)), servers = brokers)
    }

    // When we send four messages
    sendFourMessagesToEachTopic()

    //Then they should be stamped with Leader Epoch 0
    var expectedLeaderEpoch = 0
    waitUntilTrue(() => messagesHaveLeaderEpoch(expectedLeaderEpoch, 0), "Broker logs should be identical")

    //Given we then bounce the leader
    brokers(0).shutdown()
    brokers(0).startup()

    //Then LeaderEpoch should now have changed from 0 -> 1
    expectedLeaderEpoch = 1
    waitForEpochChangeTo(topic1, 0, expectedLeaderEpoch)
    waitForEpochChangeTo(topic2, 0, expectedLeaderEpoch)

    //Given we now send messages
    sendFourMessagesToEachTopic()

    //The new messages should be stamped with LeaderEpoch = 1
    waitUntilTrue(() => messagesHaveLeaderEpoch(expectedLeaderEpoch, 4), "Broker logs should be identical")
  }

  def waitForEpochChangeTo(topic: String, partition: Int, epoch: Int): Boolean = {
    TestUtils.waitUntilTrue(() => {
      brokers(0).metadataCache.getPartitionInfo(topic, partition) match {
        case Some(m) => m.leaderIsrAndControllerEpoch.leaderAndIsr.leaderEpoch == epoch
        case None => false
      }
    }, "Epoch didn't change")
  }

  def messagesHaveLeaderEpoch(expectedLeaderEpoch: Int, minOffset: Int): Boolean = {
    var result = true
    for (topic <- List(topic1, topic2)) {
      val tp = new TopicPartition(topic, 0)
      val leo = brokers.head.getLogManager().getLog(tp).get.logEndOffset
      result = result && leo > 0 && brokers.forall { broker =>
        broker.getLogManager().getLog(tp).get.logSegments.iterator.forall { segment =>
          val deepEntries = segment.read(minOffset, None, Integer.MAX_VALUE).records.deepEntries.iterator()
          scala.collection.JavaConversions.asScalaIterator(deepEntries).forall { msg =>
            info("tp:" + tp + " offset:" + msg.offset + " => LeaderEpoch: " + msg.record().leaderEpoch())
            expectedLeaderEpoch == msg.record().leaderEpoch()
          }
        }
      }
    }
    result
  }

  def sendFourMessagesToEachTopic() = {
    val testMessageList1 = List("test1", "test2", "test3", "test4")
    val testMessageList2 = List("test5", "test6", "test7", "test8")
    val producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(brokers), retries = 5, keySerializer = new StringSerializer, valueSerializer = new StringSerializer)
    val records =
      testMessageList1.map(m => new ProducerRecord(topic1, m, m)) ++
        testMessageList2.map(m => new ProducerRecord(topic2, m, m))
    records.map(producer.send).foreach(_.get)
    producer.close()
  }
}
