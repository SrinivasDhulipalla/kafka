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

import kafka.server.epoch.OffsetsForLeaderEpoch
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.{Epoch, EpochEndOffset}
import org.easymock.EasyMock._
import org.junit.Test
import org.junit.Assert._

import scala.collection.JavaConverters._
import scala.collection.mutable

class OffsetsForLeaderEpochTest {

  @Test
  def shouldGetEpochsFromReplicaMarshallingWireAndInternalForms(): Unit = {
    val replicaManager = createNiceMock(classOf[kafka.server.ReplicaManager])
    val replica = createNiceMock(classOf[kafka.cluster.Replica])
    val cache = createNiceMock(classOf[kafka.server.epoch.LeaderEpochCache])

    //Given
    val topic = "topic"
    val partition = 1
    val offset = 42
    val epochRequested = 5
    val request = mutable.Map(topic -> List(new Epoch(partition, epochRequested)).asJava).asJava

    //Stubs
    expect(replicaManager.getReplica(new TopicPartition(topic, partition))).andReturn(Some(replica))
    expect(replica.epochs).andReturn(Some(cache))
    expect(cache.endOffsetFor(epochRequested)).andReturn(offset)
    replay(replica, replicaManager, cache)

    //When
    val response = OffsetsForLeaderEpoch.getResponseFor(replicaManager, request)

    //Then
    assertEquals(List(new EpochEndOffset(Errors.NONE, 1, offset)).asJava, response.get(topic))
  }

  @Test
  def shouldGetEpochsFromReplicaMarshallingWireAndInternalFormsWithMultipleTopics(): Unit = {
    val replicaManager = createNiceMock(classOf[kafka.server.ReplicaManager])
    val replica = createNiceMock(classOf[kafka.cluster.Replica])
    val cache = createNiceMock(classOf[kafka.server.epoch.LeaderEpochCache])

    //Given
    val p1 = 1
    val offset = 42
    val epochRequested = 5

    val request = mutable.Map(
      "topic1" -> List(new Epoch(p1, epochRequested)).asJava,
      "topic2" -> List(new Epoch(p1, epochRequested)).asJava
    ).asJava

    //Stubs
    expect(replicaManager.getReplica(new TopicPartition("topic1", p1))).andReturn(Some(replica))
    expect(replicaManager.getReplica(new TopicPartition("topic2", p1))).andReturn(Some(replica))
    expect(replica.epochs).andReturn(Some(cache)).anyTimes()
    expect(cache.endOffsetFor(epochRequested)).andReturn(offset).anyTimes()
    replay(replica, replicaManager, cache)

    //When
    val response = OffsetsForLeaderEpoch.getResponseFor(replicaManager, request)

    //Then
    assertEquals(List(new EpochEndOffset(Errors.NONE, 1, offset)).asJava, response.get("topic1"))
    assertEquals(List(new EpochEndOffset(Errors.NONE, 1, offset)).asJava, response.get("topic2"))
  }

  @Test
  def shouldGetEpochsFromReplicaMarshallingWireAndInternalFormsWithMultiplePartitions(): Unit = {
    val replicaManager = createNiceMock(classOf[kafka.server.ReplicaManager])
    val replica1 = createNiceMock(classOf[kafka.cluster.Replica])
    val replica2 = createNiceMock(classOf[kafka.cluster.Replica])
    val cache1 = createNiceMock(classOf[kafka.server.epoch.LeaderEpochCache])
    val cache2 = createNiceMock(classOf[kafka.server.epoch.LeaderEpochCache])

    //Given
    val p1 = 1
    val p2 = 2
    val offset1 = 42
    val offset2 = 64
    val epochRequested = 5

    val request = mutable.Map(
      "topic" -> List(new Epoch(p1, epochRequested), new Epoch(p2, epochRequested)).asJava
    ).asJava

    //Stubs
    expect(replicaManager.getReplica(new TopicPartition("topic", p1))).andReturn(Some(replica1))
    expect(replicaManager.getReplica(new TopicPartition("topic", p2))).andReturn(Some(replica2))
    expect(replica1.epochs).andReturn(Some(cache1))
    expect(replica2.epochs).andReturn(Some(cache2))
    expect(cache1.endOffsetFor(epochRequested)).andReturn(offset1)
    expect(cache2.endOffsetFor(epochRequested)).andReturn(offset2)
    replay(replica1, replica2, replicaManager, cache1, cache2)

    //When
    val response = OffsetsForLeaderEpoch.getResponseFor(replicaManager, request)

    //Then
    assertEquals(List(
      new EpochEndOffset(Errors.NONE, 1, offset1),
      new EpochEndOffset(Errors.NONE, 2, offset2)).asJava
      , response.get("topic"))
  }

  @Test
  def shouldReturnNoLeaderForPartitionIfReplicaIsNotDefinedLocally(): Unit = {
    val replicaManager = createNiceMock(classOf[kafka.server.ReplicaManager])

    //Given
    val topic = "topic"
    val partition = 1
    val epochRequested = 5
    val request = mutable.Map(topic -> List(new Epoch(partition, epochRequested)).asJava).asJava

    //Stubs
    expect(replicaManager.getReplica(new TopicPartition(topic, partition))).andReturn(None)
    replay(replicaManager)

    //When
    val response = OffsetsForLeaderEpoch.getResponseFor(replicaManager, request)

    //Then
    assertEquals(List(new EpochEndOffset(Errors.NOT_LEADER_FOR_PARTITION, partition, UNDEFINED_OFFSET)).asJava, response.get(topic))
  }
}