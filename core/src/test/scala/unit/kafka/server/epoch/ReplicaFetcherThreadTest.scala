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

import kafka.server.epoch.LeaderEpochs
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.common.requests.FetchResponse.PartitionData
import kafka.cluster.{BrokerEndPoint, Replica}
import kafka.server._
import kafka.utils.TestUtils
import org.apache.kafka.clients.{ClientRequest, ClientResponse, MockClient}
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.requests.AbstractRequest.Builder
import org.apache.kafka.common.requests.{AbstractRequest, EpochEndOffset, OffsetForLeaderEpochResponse}
import org.apache.kafka.common.utils.{SystemTime, Time}
import org.easymock.{Capture, CaptureType}
import org.easymock.EasyMock._
import org.junit.Test
import org.junit.Assert._

import scala.collection.JavaConverters._
import scala.collection.Map

class ReplicaFetcherThreadTest {

  private val tp1 = new TopicPartition("topic1", 0)
  private val tp2 = new TopicPartition("topic2", 1)

  @Test
  def shouldFetchLeaderEpochOnFirstFetchOnly(): Unit = {
    val configs = TestUtils.createBrokerConfigs(1, "localhost:1234").map(KafkaConfig.fromProps)

    //Setup all dependencies
    val quota = createNiceMock(classOf[kafka.server.ReplicationQuotaManager])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochs])
    val logManager = createMock(classOf[kafka.log.LogManager])
    val replica = createNiceMock(classOf[Replica])
    val replicaManager = createMock(classOf[kafka.server.ReplicaManager])

    expect(logManager.truncateTo(anyObject())).once //This one is important
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(leaderEpochs.epoch).andReturn(5)
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.getReplica(tp1, 0)).andReturn(Some(replica)).anyTimes()
    expect(replicaManager.getReplica(tp2, 0)).andReturn(Some(replica)).anyTimes()
    replay(leaderEpochs, replicaManager, logManager, quota, replica)

    //Define the offsets for the OffsetsForLeaderEpochResponse
    val offsets = Map("topic1" -> List(new EpochEndOffset(0, 0, 1), new EpochEndOffset(0, 1, 1)).asJava).asJava

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsets, endPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, endPoint, configs(0), replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(tp1 -> 0, tp2 -> 0))

    //Do two loops
    thread.doWork()
    thread.doWork()

    //Assert that truncate to is called only once (despite two loops)
    verify(logManager)

    //Assert we only send one epoch request but two regular fetches
    assertEquals(1, mockNetwork.epochFetchCount)
    assertEquals(2, mockNetwork.fetchCount)
  }


  @Test
  def shouldTruncateToOffsetSpecifiedInEpochOffsetResponse(): Unit = {

    //Create a capture to track what partitions/offsets are truncated
    val truncated: Capture[Map[TopicPartition, Long]] = newCapture(CaptureType.ALL)

    // Setup all the dependencies
    val configs = TestUtils.createBrokerConfigs(1, "localhost:1234").map(KafkaConfig.fromProps)
    val quota = createNiceMock(classOf[kafka.server.ReplicationQuotaManager])
    val leaderEpochs = createNiceMock(classOf[LeaderEpochs])
    val logManager = createMock(classOf[kafka.log.LogManager])
    val replica = createNiceMock(classOf[Replica])
    val replicaManager = createMock(classOf[kafka.server.ReplicaManager])

    expect(logManager.truncateTo(capture(truncated))).once
    expect(replica.epochs).andReturn(Some(leaderEpochs)).anyTimes()
    expect(leaderEpochs.epoch).andReturn(5)
    expect(replicaManager.logManager).andReturn(logManager).anyTimes()
    expect(replicaManager.getReplica(tp1, 0)).andReturn(Some(replica)).anyTimes()
    expect(replicaManager.getReplica(tp2, 0)).andReturn(Some(replica)).anyTimes()
    replay(leaderEpochs, replicaManager, logManager, quota, replica)

    //Define the offsets for the OffsetsForLeaderEpochResponse, these are used for truncation
    val offsetsReply = Map(
      "topic1" -> List(
        new EpochEndOffset(0, 0, 156)
      ).asJava,
      "topic2" -> List(
        new EpochEndOffset(0, 1, 172)
      ).asJava
    ).asJava

    //Create the thread
    val endPoint = new BrokerEndPoint(0, "localhost", 1000)
    val mockNetwork = new ReplicaFetcherMockBlockingSend(offsetsReply, endPoint, new SystemTime())
    val thread = new ReplicaFetcherThread("bob", 0, endPoint, configs(0), replicaManager, new Metrics(), new SystemTime(), quota, Some(mockNetwork))
    thread.addPartitions(Map(tp1 -> 0, tp2 -> 0))

    //Run it
    thread.doWork()

    //We should have truncated to the offsets in the response
    assertEquals(156, truncated.getValue.get(tp1).get)
    assertEquals(172, truncated.getValue.get(tp2).get)
  }
}



class ReplicaFetcherMockBlockingSend(offsets: java.util.Map[String, java.util.List[EpochEndOffset]], destination: BrokerEndPoint, time: Time) extends BlockingSend {
  private val client = new MockClient(new SystemTime)
  var fetchCount = 0
  var epochFetchCount = 0

  override def sendRequest(requestBuilder: Builder[_ <: AbstractRequest]): ClientResponse = {

    //Send the request to the mock client
    val clientRequest = request(requestBuilder)
    client.send(clientRequest, time.milliseconds())

    //Create a suitable response based on the API key
    val response = requestBuilder.apiKey() match {
      case ApiKeys.OFFSET_FOR_LEADER_EPOCH =>
        epochFetchCount += 1
        new OffsetForLeaderEpochResponse(
          new OffsetForLeaderEpochResponse(offsets).toStruct())

      case ApiKeys.FETCH =>
        fetchCount += 1
        new FetchResponse(
          new FetchResponse(new java.util.LinkedHashMap[TopicPartition, PartitionData], 0).toStruct)
    }

    //Use mock client to create the appropriate response object
    client.respondFrom(response, new Node(destination.id, destination.host, destination.port))
    client.poll(30, time.milliseconds()).iterator().next()
  }

  private def request(requestBuilder: Builder[_ <: AbstractRequest]): ClientRequest = {
    client.newClientRequest(
      destination.id.toString,
      requestBuilder,
      time.milliseconds(),
      true)
  }

  override def close(): Unit = {}
}