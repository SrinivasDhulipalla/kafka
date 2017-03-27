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

import java.io.{File, RandomAccessFile}
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.log.Log
import kafka.server.KafkaConfig._
import kafka.server.KafkaServer
import kafka.tools.DumpLogSegments
import kafka.utils.{CoreUtils, Logging, TestUtils}
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{Record, RecordBatch}
import org.apache.kafka.test.MockDeserializer
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer => Buffer}

/**
  *
  * These tests were written to assert the addition of leader epochs to messages solve the
  * problems with the replication protocol described in KIP-101.
  *
  * The tests map to the two scenarios described in the KIP:
  * https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation
  *
  */
class EpochDrivenReplicationProtocolAcceptanceTest extends ZooKeeperTestHarness with Logging {

  val msg = new Array[Byte](1000)
  val msgBigger = new Array[Byte](10000)
  var brokers: Seq[KafkaServer] = null
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null
  val topic = "topic1"
  var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = null

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
  def shouldFollowLeaderEpochBasicWorkflow(): Unit = {

    //Given 2 brokers
    brokers = (100 to 101).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }

    //A single partition topic with 2 replicas
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, Map(0 -> Seq(100, 101)))
    producer = createNewProducer(getBrokerListStrFromServers(brokers), retries = 50, acks = -1)
    val tp = new TopicPartition(topic, 0)

    //When one record is written to the leader
    producer.send(new ProducerRecord(topic, 0, null, msg)).get

    //The message should have epoch 0 on it in both leader and follower
    assertEquals(0, latestRecord(leader).partitionLeaderEpoch())
    assertEquals(0, latestRecord(follower).partitionLeaderEpoch())

    //Both leader and follower should have recorded Epoch 0 at Offset 0
    assertEquals(Buffer(EpochEntry(0, 0)), epochCache(leader).epochEntries())
    assertEquals(Buffer(EpochEntry(0, 0)), epochCache(follower).epochEntries())

    //Bounce the follower
    bounce(follower)
    awaitISR(tp)

    //TODO: Not sure why this is or whether it is correct. Ideally the leader epoch shouldn't be affected by the follower bounce.
    //Epochs on leader in increase
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1)), epochCache(leader).epochEntries())
    assertEquals(Buffer(EpochEntry(0, 0)), epochCache(follower).epochEntries())

    //Send a message
    producer.send(new ProducerRecord(topic, 0, null, msg)).get

    //Epoch1 should now propagate to the follower with the written message
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1)), epochCache(leader).epochEntries())
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1)), epochCache(follower).epochEntries())

    //The new message should have epoch 1 stamped
    assertEquals(1, latestRecord(leader).partitionLeaderEpoch())
    assertEquals(1, latestRecord(follower).partitionLeaderEpoch())

    //Bounce the leader Epoch -> 2
    bounce(leader)
    awaitISR(tp)

    //Epochs 2 should be added to the leader, but not on the follower (yet)
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1), EpochEntry(2, 2)), epochCache(leader).epochEntries())
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1)), epochCache(follower).epochEntries())

    //Send a message
    producer.send(new ProducerRecord(topic, 0, null, msg)).get

    //This should be stamped as epoch 2 on both leader & follower
    assertEquals(2, latestRecord(leader).partitionLeaderEpoch())
    assertEquals(2, latestRecord(follower).partitionLeaderEpoch())

    //The propagation of this message via replication should increase the leader epoch
    // on the follower, so it now matches the leader
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1), EpochEntry(2, 2)), epochCache(leader).epochEntries())
    assertEquals(Buffer(EpochEntry(0, 0), EpochEntry(1, 1), EpochEntry(2, 2)), epochCache(follower).epochEntries())
  }

  @Test
  def shouldNotAllowDivergentLogs(): Unit = {

    //Given two brokers
    brokers = (100 to 101).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }
    //A single partition topic with 2 replicas
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, Map(
      0 -> Seq(100, 101)
    ))
    producer = createProducer()

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
    deleteMessagesFromLogFile(5 * msg.length, brokers(0), 0)

    //Start broker 100 again
    brokers(0).startup()

    //Bounce the producer (this is required, although I'm unsure as to why?)
    producer.close()
    producer = createProducer()

    //Write ten larger messages
    (0 until 10).foreach { _ =>
      producer.send(new ProducerRecord(topic, 0, null, msgBigger))
      producer.flush()
    }

    //Start broker 101
    brokers(1).startup()

    //Wait for replication to resync
    waitForLogsToMatch(brokers(0), brokers(1))

    assertEquals("Log files should match Broker0 vs Broker 1", getLogFile(brokers(0), 0).length, getLogFile(brokers(1), 0).length)
  }


  //This is essentially the use case described in https://issues.apache.org/jira/browse/KAFKA-3919
  //Is currently in "failing mode"
  @Test
  def offsetsShouldNotGoBackwards(): Unit = {

    //Given two brokers
    brokers = (100 to 101).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }
    //A single partition topic with 2 replicas
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, Map(
      0 -> Seq(100, 101)
    ))
    producer = bufferingProducer()

    //Write 100 messages in batches of 10
    (0 until 10).foreach { i =>
      (0 until 10).foreach { j =>
        producer.send(new ProducerRecord(topic, 0, null, msg))
      }
      producer.flush()
    }

    //Stop the brokers
    brokers.foreach { b => b.shutdown() }

    //Delete the clean shutdown file to simulate crash
    new File(brokers(0).config.logDirs(0), Log.CleanShutdownFile).delete()

    //Delete half the messages from the log file
    deleteMessagesFromLogFile(getLogFile(brokers(0), 0).length() / 2, brokers(0), 0)

    //Start broker 100 again
    brokers(0).startup()

    //Bounce the producer (this is required, although I'm unsure as to why?)
    producer.close()
    producer = bufferingProducer()

    //Write two large batches of messages. This will ensure that the LeO of the follower's log aligns with the middle
    //of the a compressed message set in the leader (which, when forwarded will result in offsets going backwards)
    (0 until 77).foreach { _ =>
      producer.send(new ProducerRecord(topic, 0, null, msg))
    }
    producer.flush()
    (0 until 77).foreach { _ =>
      producer.send(new ProducerRecord(topic, 0, null, msg))
    }
    producer.flush()

    printSegments()

    //Start broker 101. When it comes up it should read a whole batch of messages from the leader.
    //As the chronology is lost we should end up with non-monatonic offsets
    brokers(1).startup()

    //Wait for replication to resync
    waitForLogsToMatch(brokers(0), brokers(1))

    printSegments()

    //Shut down broker 100, so we read from broker 101 which should have corrupted
    brokers(0).shutdown()

    //Search to see if we have non-monotonic offsets in the log
    startConsumer()
    val records = consumer.poll(1000).asScala
    var prevOffset = -1L
    records.foreach { r =>
      assertTrue(s"Offset $prevOffset came before ${r.offset} ", r.offset > prevOffset)
      prevOffset = r.offset
    }

    //Are the files identical?
    assertEquals("Log files should match Broker0 vs Broker 1", getLogFile(brokers(0), 0).length, getLogFile(brokers(1), 0).length)
  }

  @Test //This test doesn't work as expected (i.e. it doesn't fail on trunk). WIP.
  def shouldSurviveFastLeaderChange(): Unit = {
    val tp = new TopicPartition(topic, 0)

    //Given 2 brokers
    brokers = (100 to 101).map { id => createServer {
      val config = createBrokerConfig(id, zkConnect)
      config.setProperty("unclean.leader.election.enable", "false")
      config.setProperty("min.insync.replicas", "2")
      fromProps(config)
    }}

    //A single partition topic with 2 replicas
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, Map(0 -> Seq(100, 101)))
    producer = createNewProducer(getBrokerListStrFromServers(brokers), retries = 50, acks = -1)

    //Kick off with a single record
    producer.send(new ProducerRecord(topic, 0, null, msg)).get
    var messagesWritten = 1

    //Now invoke the fast leader change bug
    (0 until 5).foreach { i =>
      val leaderId = zkUtils.getLeaderForPartition(topic, 0).get
      val leader = brokers.filter(_.config.brokerId == leaderId)(0)
      val follower = brokers.filter(_.config.brokerId != leaderId)(0)

      producer.send(new ProducerRecord(topic, 0, null, msg)).get
      messagesWritten += 1

      //As soon as it replicates, bounce the follower
      bounce(follower)

      log(leader, follower)
      awaitISR(tp)

      //Then bounce the leader
      bounce(leader)

      log(leader, follower)
      awaitISR(tp)

      //Ensure no data was lost
      assertTrue(brokers.forall { broker => getLog(broker, 0).logEndOffset == messagesWritten })
    }
  }

  def log(leader: KafkaServer, follower: KafkaServer): Unit = {
    info(s"Bounce complete for follower ${follower.config.brokerId}")
    info(s"Leader: leo${leader.config.brokerId}: " + getLog(leader, 0).logEndOffset + " cache: " + epochCache(leader).epochEntries())
    info(s"Follower: leo${follower.config.brokerId}: " + getLog(follower, 0).logEndOffset + " cache: " + epochCache(follower).epochEntries())
  }

  def waitForLogsToMatch(b1: KafkaServer, b2: KafkaServer, partition: Int = 0): Unit = {
    while (getLog(b1, partition).logEndOffset != getLog(b2, partition).logEndOffset) {
      Thread.sleep(1000)
    }
  }

  def printSegments(): Unit = {
    info("Broker0:")
    DumpLogSegments.main(Seq("--files", getLogFile(brokers(0), 0).getCanonicalPath).toArray)
    info("Broker1:")
    DumpLogSegments.main(Seq("--files", getLogFile(brokers(1), 0).getCanonicalPath).toArray)
  }

  def startConsumer(): KafkaConsumer[Array[Byte], Array[Byte]] = {
    val consumerConfig = new Properties()
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerListStrFromServers(brokers))
    consumerConfig.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, String.valueOf(getLogFile(brokers(1), 0).length() * 2))
    consumerConfig.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(getLogFile(brokers(1), 0).length() * 2))
    consumer = new KafkaConsumer(consumerConfig, new MockDeserializer, new MockDeserializer)
    consumer.assign(List(new TopicPartition(topic, 0)).asJava)
    consumer.seek(new TopicPartition(topic, 0), 0)
    consumer
  }

  def deleteMessagesFromLogFile(bytes: Long, broker: KafkaServer, partitionId: Int): Unit = {
    val logFile = getLogFile(broker, partitionId)
    val writable = new RandomAccessFile(logFile, "rwd")
    writable.setLength(logFile.length() - bytes)
    writable.close()
  }

  def bufferingProducer(): KafkaProducer[Array[Byte], Array[Byte]] = {
    createNewProducer(getBrokerListStrFromServers(brokers), retries = 5, acks = -1, lingerMs = 10000,
      props = Option(CoreUtils.propsWith(
        (ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(msg.length * 1000))
        , (ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
      )))
  }

  def getLogFile(broker: KafkaServer, partition: Int): File = {
    val log: Log = getLog(broker, partition)
    log.flush()
    log.dir.listFiles.filter(_.getName.endsWith(".log"))(0)
  }

  def getLog(broker: KafkaServer, partition: Int): Log = {
    broker.logManager.logsByTopicPartition.get(new TopicPartition(topic, partition)).get
  }

  def bounce(follower: KafkaServer): Unit = {
    follower.shutdown()
    follower.startup()
    producer = createProducer()
  }

  def epochCache(broker: KafkaServer): LeaderEpochFileCache = {
    getLog(broker, 0).leaderEpochCache.asInstanceOf[LeaderEpochFileCache]
  }

  def latestRecord(leader: KafkaServer, offset: Int = -1, partition: Int = 0): RecordBatch = {
    getLog(leader, partition).activeSegment.read(0, None, Integer.MAX_VALUE)
      .records.batches().asScala.toSeq.last
  }

  def readRecordAtOffset(leader: KafkaServer, offset: Int, partition: Int = 0): RecordBatch = {
    getLog(leader, partition).activeSegment.read(0, None, Integer.MAX_VALUE)
      .records.batches().asScala.toSeq(offset)
  }

  def awaitISR(tp: TopicPartition): Boolean = {
    TestUtils.waitUntilTrue(() => {
      leader.replicaManager.getReplicaOrException(tp).partition.inSyncReplicas.map(_.brokerId).size == 2
    }, "")
  }


  def createProducer(): KafkaProducer[Array[Byte], Array[Byte]] = {
    createNewProducer(getBrokerListStrFromServers(brokers), retries = 5, acks = -1)
  }

  def leader(): KafkaServer = {
    assertEquals(2, brokers.size)
    val leaderId = zkUtils.getLeaderForPartition(topic, 0).get
    brokers.filter(_.config.brokerId == leaderId)(0)
  }

  def follower(): KafkaServer = {
    assertEquals(2, brokers.size)
    val leader = zkUtils.getLeaderForPartition(topic, 0).get
    brokers.filter(_.config.brokerId != leader)(0)
  }
}
