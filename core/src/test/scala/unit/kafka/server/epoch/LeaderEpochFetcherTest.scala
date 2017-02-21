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

import kafka.server.{BlockingSend}
import kafka.server.epoch.{LeaderEpochFetcher, PartitionEpoch}
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.{AbstractRequest, EpochEndOffset, OffsetForLeaderEpochResponse}
import org.easymock.EasyMock._
import org.junit.Test

import scala.collection.JavaConverters._


class LeaderEpochFetcherTest {

  @Test  //TODO finish me
  def shouldSendEpochRequest(): Unit = {
    val sender = createMock(classOf[BlockingSend])
    val clientResponse = createMock(classOf[ClientResponse])

    expect(sender.sendRequest(anyObject().asInstanceOf[AbstractRequest.Builder[_ <: AbstractRequest]]))
      .andStubReturn(clientResponse)

    val response1 = createMock(classOf[OffsetForLeaderEpochResponse])
    expect(response1.responses()).andStubReturn(Map("topic1" -> List(new EpochEndOffset(0, 0, 156)).asJava).asJava)

    val response2 = createMock(classOf[OffsetForLeaderEpochResponse])
    expect(response2.responses()).andStubReturn(Map("topic1" -> List(new EpochEndOffset(0, 1, 172)).asJava).asJava)

    expect(clientResponse.responseBody()).andStubReturn(response1)
    expect(clientResponse.responseBody()).andStubReturn(response2)

    replay(sender)
    replay(clientResponse)
    replay(response1)
    replay(response2)

    val fetcher = new LeaderEpochFetcher(sender)
    val tp0 = new TopicPartition("topic1", 0)
    val tp1 = new TopicPartition("topic1", 1)

    val response = fetcher.fetchLeaderEpochs(Set(
      new PartitionEpoch(tp0, 5),
      new PartitionEpoch(tp1, 7))
    )

//    assertEquals(Map(tp0 -> 156, tp1 -> 172), response)

  }

}
