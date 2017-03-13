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

import kafka.server.ReplicaManager
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.requests.{Epoch, EpochEndOffset, OffsetForLeaderEpochRequest}

import scala.collection.JavaConverters._

object OffsetsForLeaderEpoch {
  val UNDEFINED_OFFSET = -1
}

class OffsetsForLeaderEpoch(replicaManager: ReplicaManager) {

  def getOffsetsForEpochs(offsetForEpoch: OffsetForLeaderEpochRequest): util.Map[String, util.List[EpochEndOffset]] = {
    offsetForEpoch.epochsByTopic().asScala.map { case (topic, epochs) =>
      (topic, lastOffsetsByEpoch(topic, epochs))
    }.asJava
  }

  private def lastOffsetsByEpoch(topic: String, epochs: util.List[Epoch]): util.List[EpochEndOffset] = {
    epochs.asScala.map { epoch =>
      replicaManager.getPartition(new TopicPartition(topic, epoch.partitionId)) match {
        case Some(p) =>
          if (p.getReplica().isDefined) {
            val offset = p.getReplica().get.epochs.get.lastOffsetFor(epoch.epoch)
            new EpochEndOffset(0, epoch.partitionId, offset)
          } else
            new EpochEndOffset(NOT_LEADER_FOR_PARTITION.code(), epoch.partitionId, OffsetsForLeaderEpoch.UNDEFINED_OFFSET)
        case None => new EpochEndOffset(REPLICA_NOT_AVAILABLE.code(), epoch.partitionId, OffsetsForLeaderEpoch.UNDEFINED_OFFSET)
      }
    }.asJava
  }
}