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
package kafka.server.epoch

import kafka.admin.AdminUtils
import kafka.server.KafkaConfig._
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils._
import kafka.utils.{Logging, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.SystemTime
import org.apache.kafka.common.{Node, TopicPartition}
import org.junit.Assert._
import org.junit.{After, Before, Test}
import kafka.server.epoch.util.TestSender

import scala.collection.JavaConverters._

class LeaderEpochIntegrationTest extends ZooKeeperTestHarness with Logging {
  var brokers: Seq[KafkaServer] = null
  val topic1 = "foo"
  val topic2 = "bar"
  val t1p0 = new TopicPartition(topic1, 0)
  val t1p1 = new TopicPartition(topic1, 1)
  val t1p2 = new TopicPartition(topic1, 2)
  val tp = t1p0
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null

  @Before
  override def setUp() {
    super.setUp()
    val props = createBrokerConfigs(2, zkConnect)
    brokers = props.map(KafkaConfig.fromProps).map(TestUtils.createServer(_))
  }

  @After
  override def tearDown() {
    brokers.foreach(_.shutdown())
    if(producer != null)
      producer.close()
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

  @Test
  def shouldSendLeaderEpochRequestAndGetAResponse(): Unit = {

    //3 brokers, put partition on 100/101 and then pretend to be 102
    brokers = (100 to 102).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic1, Map(
      0 -> Seq(100),
      1 -> Seq(101)
    ))

    //Send messages equally to the two partitions
    producer = createNewProducer(getBrokerListStrFromServers(brokers), retries = 5, acks = -1)
    (0 until 100).foreach { _ =>
      producer.send(new ProducerRecord(topic1, 0, null, "IHeartLogs".getBytes))
      producer.send(new ProducerRecord(topic1, 1, null, "IReallyDo".getBytes))
    }
    producer.flush()

    val fetcher = new LeaderOffsetsForEpochsFetcher(sender(brokers(2), brokers(0)))
    val epochsRequested = Set(new PartitionEpoch(t1p0, 0), new PartitionEpoch(t1p1, 0))

    //When
    val offsetsForEpochs = fetcher.leaderOffsetsFor(epochsRequested)

    //Then end offset should be 100 for tp1 (matching leo)
    assertEquals(100, offsetsForEpochs(t1p0).endOffset)

    //Then the unsupported partition should return an error
    assertTrue(offsetsForEpochs(t1p1).hasError)
    assertEquals(NOT_LEADER_FOR_PARTITION, offsetsForEpochs(t1p1).error)
    assertEquals(UNDEFINED_OFFSET, offsetsForEpochs(t1p1).endOffset)
  }

  @Test
  def shouldIncreaseLeaderEpochBetweenLeaderRestarts(): Unit ={

    //Setup: we are only interested in the single partition on broker 101
    brokers = Seq(100, 101).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }
    def epoch(e: Int) = Set(new PartitionEpoch(tp, e))
    def leo() = brokers(1).replicaManager.getReplica(tp).get.logEndOffset.messageOffset
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, tp.topic, Map(tp.partition -> Seq(101)))
    producer = createNewProducer(getBrokerListStrFromServers(brokers), retries = 5, acks = -1)

    //1. Given a single message
    producer.send(new ProducerRecord(tp.topic, tp.partition, null, "IHeartLogs".getBytes)).get
    var fetcher = new LeaderOffsetsForEpochsFetcher(sender(brokers(0), brokers(1)))

    //Then epoch should be 0 and leo: 1
    var offset = fetcher.leaderOffsetsFor(epoch(0))(tp).endOffset()
    assertEquals(1, offset)
    assertEquals(leo(), offset)


    //2. When broker is bounced
    brokers(1).shutdown()
    brokers(1).startup()

    producer.send(new ProducerRecord(tp.topic, tp.partition, null, "IHeartLogs".getBytes)).get
    fetcher = new LeaderOffsetsForEpochsFetcher(sender(brokers(0), brokers(1)))


    //Then epoch 0 should still be the start offset of epoch 1
    offset = fetcher.leaderOffsetsFor(epoch(0))(tp).endOffset()
    assertEquals(1, offset)

    //Then epoch 2 should be the leo (NB: The leader epoch goes up in factors of 2)
    assertEquals(2, fetcher.leaderOffsetsFor(epoch(2))(tp).endOffset())
    assertEquals(leo(), fetcher.leaderOffsetsFor(epoch(2))(tp).endOffset())


    //3. When broker is bounced again
    brokers(1).shutdown()
    brokers(1).startup()

    producer.send(new ProducerRecord(tp.topic, tp.partition, null, "IHeartLogs".getBytes)).get
    fetcher = new LeaderOffsetsForEpochsFetcher(sender(brokers(0), brokers(1)))


    //Then Epoch 0 should still map to offset 1
    assertEquals(1, fetcher.leaderOffsetsFor(epoch(0))(tp).endOffset())

    //Then Epoch 2 should still map to offset 2
    assertEquals(2, fetcher.leaderOffsetsFor(epoch(2))(tp).endOffset())

    //Then Epoch 4 should still map to offset 2
    assertEquals(3, fetcher.leaderOffsetsFor(epoch(4))(tp).endOffset())
    assertEquals(leo(), fetcher.leaderOffsetsFor(epoch(4))(tp).endOffset())

    //Adding some extra assertions here to save test setup.
    shouldSupportRequestsForEpochsNotOnTheLeader(fetcher)
  }

  //Appended onto the previous test to save on setup cost.
  def shouldSupportRequestsForEpochsNotOnTheLeader(fetcher: LeaderOffsetsForEpochsFetcher): Unit ={
    def epoch(e: Int) = Set(new PartitionEpoch(t1p0, e))

    /**
      * Asking for an epoch not present on the leader should return the
      * next matching epoch, unless there isn't any, which should return
      * undefined.
      */

    val epoch1 = epoch(1)
    assertEquals(1, fetcher.leaderOffsetsFor(epoch1)(t1p0).endOffset())

    val epoch3 = epoch(3)
    assertEquals(2, fetcher.leaderOffsetsFor(epoch3)(t1p0).endOffset())

    val epoch5 = epoch(5)
    assertEquals(-1, fetcher.leaderOffsetsFor(epoch5)(t1p0).endOffset())
  }

  def sender(from: KafkaServer, to: KafkaServer): TestSender = {
    val endPoint = from.metadataCache.getAliveBrokers.find(_.id == to.config.brokerId).get.getBrokerEndPoint(from.config.interBrokerListenerName)
    val destinationNode = new Node(endPoint.id, endPoint.host, endPoint.port)
    new TestSender(destinationNode, from.config, new Metrics(), new SystemTime())
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
          if (segment.read(minOffset, None, Integer.MAX_VALUE) == null) {
            false
          } else {
            segment.read(minOffset, None, Integer.MAX_VALUE)
              .records.batches().iterator().asScala.forall (
              expectedLeaderEpoch == _.partitionLeaderEpoch()
            )
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