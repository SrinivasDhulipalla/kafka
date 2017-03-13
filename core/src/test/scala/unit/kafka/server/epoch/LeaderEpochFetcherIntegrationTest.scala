/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package unit.kafka.server.epoch

import kafka.admin.AdminUtils
import kafka.server.KafkaConfig._
import kafka.server.KafkaServer
import kafka.server.epoch.{LeaderEpochFetcher, PartitionEpoch}
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.SystemTime
import org.apache.kafka.common.{Node, TopicPartition}
import org.junit.Assert._
import org.junit.{After, Before, Test}
import kafka.server.epoch.OffsetsForLeaderEpoch
import unit.kafka.server.epoch.util.TestSender

class LeaderEpochFetcherIntegrationTest extends ZooKeeperTestHarness {
  var brokers: Seq[KafkaServer] = null
  val topic = "topic1"
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null

  @Before
  override def setUp() {
    super.setUp()
  }

  @After
  override def tearDown() {
    brokers.par.foreach(_.shutdown())
    producer.close()
    super.tearDown()
  }

  @Test
  def shouldGetAResponse(): Unit = {

    //3 brokers, put partition on 100/101 and then pretend to be 102
    brokers = (100 to 102).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, Map(
      0 -> Seq(100),
      1 -> Seq(101)
    ))

    //Send messages equally to the two partitions
    producer = createNewProducer(getBrokerListStrFromServers(brokers), retries = 5, acks = -1)
    (0 until 100).foreach { _ =>
      producer.send(new ProducerRecord(topic, 0, null, "test0".getBytes))
      producer.send(new ProducerRecord(topic, 1, null, "test1".getBytes))
    }
    producer.flush()

    val fetcher = new LeaderEpochFetcher(sender(brokers(0)))

    val tp1 = new TopicPartition(topic, 0)
    val tp2 = new TopicPartition(topic, 2)
    val epochsRequested = Set(new PartitionEpoch(tp1, 5), new PartitionEpoch(tp2, 7))

    //When
    val responses = fetcher.fetchLeaderEpochs(epochsRequested)

    //Then end offset should be defined as 0 for tp1
    assertEquals(0, responses.get(tp1).get.endOffset)

    //Then end offset should be undefined for tp2, as well as reporting an error
    assertTrue(responses.get(tp2).get.hasError)
    assertEquals(Errors.REPLICA_NOT_AVAILABLE.code(), responses.get(tp2).get.error)
    assertEquals(OffsetsForLeaderEpoch.UNDEFINED_OFFSET, responses.get(tp2).get.endOffset)

    assertEquals(2, responses.size)
  }

  //TODO more here please

  def sender(broker: KafkaServer): TestSender = {
    val endPoint = broker.metadataCache.getAliveBrokers.find(_.id == 100).get.getBrokerEndPoint(broker.config.interBrokerListenerName)
    val destinationNode = new Node(endPoint.id, endPoint.host, endPoint.port)
    new TestSender(destinationNode, broker.config, new Metrics(), new SystemTime())
  }
}
