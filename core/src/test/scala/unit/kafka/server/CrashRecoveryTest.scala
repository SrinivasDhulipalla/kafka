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

import java.io.{File, RandomAccessFile}

import kafka.admin.AdminUtils
import kafka.log.Log
import kafka.server.KafkaConfig._
import kafka.server.KafkaServer
import kafka.utils.{CoreUtils, TestUtils}
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.junit.{After, Before, Test}
import org.junit.Assert.assertEquals

class CrashRecoveryTest extends ZooKeeperTestHarness {

  val msg = new Array[Byte](1000)
  val msgBigger = new Array[Byte](10000)
  var brokers: Seq[KafkaServer] = null
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null
  val topic = "topic1"

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

  //        , (ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")

  @Test
  def shouldNotAllowDivergentLogs(): Unit = {

    //Given two brokers
    brokers = (100 to 101).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }
    //A single partition topic with 2 replicas
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, Map(
      0 -> Seq(100, 101)
    ))
    producer = createNewProducer(getBrokerListStrFromServers(brokers), retries = 5, acks = -1)

    //Write 10 messages
    (0 until 10).foreach { i =>
      producer.send(new ProducerRecord(topic, 0, null, msg))
      producer.flush()
    }

    //Stop the brokers
    brokers.foreach { b => b.shutdown() }

    //Delete the clean shutdown file to simulate crash
    new File(brokers(0).config.logDirs(0), Log.CleanShutdownFile).delete()

    //Delete 5 messages from the leader's log on 100
    deleteMessagesFromLogFile(5, brokers(0), 0)

    //Start broker 100 again
    brokers(0).startup()

    //Bounce the producer (this is required, although I'm unsure as to why?)
    producer.close()
    producer = createNewProducer(getBrokerListStrFromServers(brokers), retries = 5, acks = -1)

    //Write ten larger messages
    (0 until 10).foreach { _ =>
      producer.send(new ProducerRecord(topic, 0, null, msgBigger))
      producer.flush()
    }

    //Start broker 101
    brokers(1).startup()

    //Wait for replication to resync
    while (getLog(brokers(0), 0).logEndOffset != getLog(brokers(1), 0).logEndOffset) Thread.sleep(100)

    //For now assert that the logs are corrupted by the expected amount. Once fixed we should assert the logs are identical
    assertEquals(getLogFile(brokers(0), 0).length, getLogFile(brokers(1), 0).length  + 5 * (msgBigger.length - msg.length))
//    assertEquals("Log files should match Broker0 vs Broker 1", getLogFile(brokers(0), 0).length, getLogFile(brokers(1), 0).length)

  }

  def deleteMessagesFromLogFile(msgs: Int, broker: KafkaServer, partitionId: Int): Unit = {
    val logFile = getLogFile(broker, partitionId)
    val writable = new RandomAccessFile(logFile, "rwd")
    writable.setLength(logFile.length() - msgs * msg.length)
    writable.close()
  }

  def getLogFile(broker: KafkaServer, partition: Int): File = {
    val log: Log = getLog(broker, partition)
    log.flush()
    log.dir.listFiles.filter(_.getName.endsWith(".log"))(0)
  }

  def getLog(broker: KafkaServer, partition: Int): Log = {
    broker.logManager.logsByTopicPartition.get(new TopicPartition(topic, partition)).get
  }
}
