/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package unit.kafka.server

import kafka.log.TempLeaderEpochStuff
import kafka.server.{FetchDataInfo, KafkaConfig, KafkaServer}
import kafka.utils.TestUtils
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.{After, Before, Test}

class LeaderEpochTest extends ZooKeeperTestHarness  {
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

  //This test currently just checks that the server assigns a temp value
  //to the message when it is written. This will change in future commits.
  @Test
  def shouldReplicateLeaderEpoch() {
    val partition = 0
    val testMessageList1 = List("test1", "test2", "test3", "test4")
    val testMessageList2 = List("test5", "test6", "test7", "test8")

    // create a topic and partition and await leadership
    for (topic <- List(topic1,topic2)) {
      createTopic(zkUtils, topic, numPartitions = 1, replicationFactor = 2, servers = brokers)
    }

    // send test messages to leader
    val producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(brokers),
      retries = 5,
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer)
    val records = testMessageList1.map(m => new ProducerRecord(topic1, m, m)) ++
      testMessageList2.map(m => new ProducerRecord(topic2, m, m))
    records.map(producer.send).foreach(_.get)
    producer.close()

    //Make sure the broker is setting the leader epoch and that propagates to the follower
    def logsMatch(): Boolean = {
      var result = true
      for (topic <- List(topic1, topic2)) {
        val tp = new TopicPartition(topic, partition)
        val leo = brokers.head.getLogManager().getLog(tp).get.logEndOffset
        result = result && leo > 0 && brokers.forall { broker =>
          broker.getLogManager().getLog(tp).get.logSegments.iterator.forall{segment =>
            val info: FetchDataInfo = segment.read(segment.baseOffset, None, Integer.MAX_VALUE)
            val deepEntries = info.records.deepEntries.iterator()
            scala.collection.JavaConversions.asScalaIterator(deepEntries).forall{ msg =>
              println("Validated that leader epoch is " +  msg.record().leaderEpoch())
              TempLeaderEpochStuff.TEMP_FIXED_SERVER_ASSIGNED_LEADER_EPOCH == msg.record().leaderEpoch()
            }
          }
        }
      }
      result
    }
    waitUntilTrue(logsMatch, "Broker logs should be identical")
  }
}
