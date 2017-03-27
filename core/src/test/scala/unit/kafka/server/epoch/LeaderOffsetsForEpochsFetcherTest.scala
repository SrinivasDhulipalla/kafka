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

import kafka.server.BlockingSend
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.{EpochEndOffset, OffsetForLeaderEpochResponse}
import org.easymock.EasyMock._
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

/**
  * Unit test for the LeaderEpochFetcher
  */
class LeaderEpochFetcherTest {

  @Test
  def shouldProcessEpochResponseFromMultiplePartitions(): Unit = {
    val sender = createMock(classOf[BlockingSend])
    val clientResponse = createMock(classOf[ClientResponse])

    val tp0 = new TopicPartition("topic1", 0)
    val tp1 = new TopicPartition("topic1", 1)

    val offsets = Map("topic1" -> List(
      new EpochEndOffset(0, 156),
      new EpochEndOffset(1, 172)
    ).asJava)

    val epochResponse = createMock(classOf[OffsetForLeaderEpochResponse])

    expect(epochResponse.responses()).andStubReturn(offsets.asJava)
    expect(sender.sendRequest(anyObject())).andStubReturn(clientResponse)
    expect(clientResponse.responseBody()).andStubReturn(epochResponse)
    replay(sender, clientResponse, epochResponse)

    val fetcher = new LeaderEpochFetcher(sender)

    val response = fetcher.leaderOffsetsFor(Set(
      new PartitionEpoch(tp0, 5),
      new PartitionEpoch(tp1, 7))
    )

    assertEquals(Map(tp0 -> EEO(tp0, 156), tp1 -> EEO(tp1, 172)), response)
  }

  @Test
  def shouldProcessEpochResponseFromMultipleTopics(): Unit = {
    val sender = createMock(classOf[BlockingSend])
    val clientResponse = createMock(classOf[ClientResponse])

    val tp0 = new TopicPartition("topic1", 0)
    val tp1 = new TopicPartition("topic2", 0)

    val offsets = Map(
      "topic1" -> List(new EpochEndOffset(0, 156)).asJava,
      "topic2" -> List(new EpochEndOffset(0, 172)).asJava
    )

    val epochResponse = createMock(classOf[OffsetForLeaderEpochResponse])

    expect(epochResponse.responses()).andStubReturn(offsets.asJava)
    expect(sender.sendRequest(anyObject())).andStubReturn(clientResponse)
    expect(clientResponse.responseBody()).andStubReturn(epochResponse)
    replay(sender, clientResponse, epochResponse)

    val fetcher = new LeaderEpochFetcher(sender)

    val response = fetcher.leaderOffsetsFor(Set(
      new PartitionEpoch(tp0, 5),
      new PartitionEpoch(tp1, 7))
    )

    assertEquals(Map(tp0 -> EEO(tp0, 156), tp1 -> EEO(tp1, 172)), response)
  }

  def EEO(tp: TopicPartition, offset: Int): EpochEndOffset = {
    new EpochEndOffset(tp.partition, offset)
  }
}