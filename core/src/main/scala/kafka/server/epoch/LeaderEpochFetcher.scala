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

import java.util
import java.util.{List => JList}

import kafka.cluster.{BrokerEndPoint, Partition}
import kafka.server.BlockingSend
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{EpochEndOffset, OffsetForLeaderEpochResponse, _}

import scala.collection.JavaConverters._
import scala.collection.{Map, Set}

class LeaderEpochFetcher(sender: BlockingSend) extends Logging{

  def fetchLeaderEpochs(partitions: Set[PartitionEpoch]): Map[TopicPartition, EpochEndOffset] = {
    val epochsByTopic = translate(partitions)
    fetchLeaderEpochs(epochsByTopic)
  }

  private def translate(partitions: Set[PartitionEpoch]): util.Map[String, JList[Epoch]] = {
    partitions.toSeq.groupBy {_.tp.topic()}
      .map { case (topic, partitionEpochs) =>
        val epochs = partitionEpochs.map { ep => new Epoch(ep.tp.partition, ep.epoch) }.asJava
        (topic, epochs)
      }.asJava
  }

  private def fetchLeaderEpochs(epochsByTopic: util.Map[String, util.List[Epoch]]): Map[TopicPartition, EpochEndOffset] = {
    val requestBuilder = new OffsetForLeaderEpochRequest.Builder(epochsByTopic)
    parseEpochs(
      sender.sendRequest(requestBuilder).responseBody.asInstanceOf[OffsetForLeaderEpochResponse]
    )
  }

  private def parseEpochs(response: OffsetForLeaderEpochResponse): Map[TopicPartition, EpochEndOffset] = {
    response
      .responses.asScala
      .flatMap { case (topic, offsets) =>
        offsets.asScala.flatMap { epoch =>
          maybeWarn(epoch)
          Map(new TopicPartition(topic, epoch.partitionId) -> epoch)
        }
      }.toMap
  }

  def maybeWarn(epochOffset: EpochEndOffset): Unit = {
    if (epochOffset.hasError)
      warn(s"OffsetForLeaderEpoch request returned an error. High Watermark will be used for truncation. The error was: "
        + epochOffset.error.message())
  }
}

trait EndpointSupplier {
  def supply(partition: Partition): BrokerEndPoint
}

case class PartitionEpoch(tp: TopicPartition, epoch: Int)
