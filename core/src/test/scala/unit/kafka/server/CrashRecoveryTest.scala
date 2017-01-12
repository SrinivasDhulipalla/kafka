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

  @Test
  def shouldFailWithCorruptedLogsAfterCrash(): Unit = {

    //Given
    brokers = (100 to 101).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, Map(
      0 -> Seq(100, 101)
    ))

    val numMessages: Int = 100

    producer = createNewProducer(getBrokerListStrFromServers(brokers), retries = 5, acks = -1, lingerMs = 1000,
      props = Option(CoreUtils.propsWith(
        (ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(msg.length * numMessages))
                  ,(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
      )))



    val start = System.currentTimeMillis()

    //Write N messages, in batches of 10
    (0 until numMessages).foreach { i =>
      producer.send(new ProducerRecord(topic, 0, null, msg))
      if (i % 10 == 9)
        producer.flush()
    }
    producer.flush()

    warn("Stopping both brokers")
    brokers.foreach { b => b.shutdown() }



    //Delete 5 messages from the leader on 100
    warn("Removing 5 messages from broker " + brokers(0).config.brokerId)
    val logFile = getLogFile(brokers(0), 0)
    warn("Length before delete: " + logFile.length)

    val oldLen = logFile.length()
    val f: RandomAccessFile = new RandomAccessFile(logFile, "rwd")
    val trimBy: Int = 5 * msg.length
    f.setLength(logFile.length() - trimBy)

    assertEquals(oldLen - trimBy, logFile.length())
    warn("File trimmed to " + f.length())
    f.close()



    //Delete the clean shutdown file to simulate crash

    val cleanShutdown: File = new File(brokers(0).config.logDirs(0), Log.CleanShutdownFile)
    cleanShutdown.delete()
    warn("did we get a clean shutdown? " + cleanShutdown.exists())


    warn("**** starting broker " + brokers(0).config.brokerId)
    brokers(0).startup()
    warn("**** started broker " + brokers(0).config.brokerId)

    //    assertEquals(oldLen - trimBy, getLogFile(brokers(0), 0).length())

    producer.close()
    producer = createNewProducer(getBrokerListStrFromServers(brokers), retries = 5, acks = -1, lingerMs = 1,
      props = Option(CoreUtils.propsWith((ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(1))
        , (ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
      )))


    warn("Broker 100 file now " + getLogFile(brokers(0), 0).length())
    warn("Broker 100 offset now : " + getLog(brokers(0), 0).logEndOffset)
    warn("writing ten messages to broker 100 (broker 101 is still down")
    (0 until 10).foreach { _ =>
      producer.send(new ProducerRecord(topic, 0, null, msg))
      producer.flush()
      warn("writing")
    }
    warn("starting flush")
    producer.flush()
    warn("flush complete")
    warn("Broker 100 file now " + getLogFile(brokers(0), 0).length())
    warn("Broker 100 offset now : " + getLog(brokers(0), 0).logEndOffset)




    warn("*****starting broker " + brokers(1).config.brokerId)
    brokers(1).startup()
    warn("*****broker started")
    Thread.sleep(1000)
    warn("file b0:0 after extra messages " + getLogFile(brokers(0), 0).length())
    warn("file b1:0 after extra messages " + getLogFile(brokers(1), 0).length())
    warn("Broker 100 offset now : " + getLog(brokers(0), 0).logEndOffset)
    warn("Broker 101 offset now : " + getLog(brokers(1), 0).logEndOffset)




    (0 until 10).foreach { _ =>
      producer.send(new ProducerRecord(topic, 0, null, msg))
      producer.flush()
      warn("final writing leo 0-0: " + getLog(brokers(0), 0).logEndOffset)
      warn("final writing leo 1-0: " + getLog(brokers(1), 0).logEndOffset)
    }

    warn("took " + (System.currentTimeMillis() - start) + " ms")
    assertEquals("LEOs should match", getLog(brokers(0), 0).logEndOffset, getLog(brokers(1), 0).logEndOffset)
    assertEquals("Log files should match Broker0 vs Broker 1", getLogFile(brokers(0), 0).length, getLogFile(brokers(1), 0).length)
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
