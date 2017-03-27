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

import java.util.{List => JList, Map => JMap}

import kafka.server.ReplicaManager
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.{Epoch, EpochEndOffset}

import scala.collection.JavaConverters._


object OffsetsForLeaderEpoch {

  def getOffsetsForEpochs(replicaManager: ReplicaManager,
                          requestedEpochInfo: JMap[String, JList[Epoch]],
                          authorised: Boolean): JMap[String, JList[EpochEndOffset]] = {
    requestedEpochInfo.asScala.map { case (topic, epochs) =>
      val lastOffsetsByEpoch = epochs.asScala.map { epoch =>
        if (authorised) {
          replicaManager.getReplica(new TopicPartition(topic, epoch.partitionId)) match {
            case Some(replica) =>
                val offset = replica.epochs.get.endOffsetFor(epoch.epoch)
                new EpochEndOffset(epoch.partitionId, offset)
            case None => new EpochEndOffset(NOT_LEADER_FOR_PARTITION, epoch.partitionId, UNDEFINED_OFFSET)
          }
        } else {
          new EpochEndOffset(Errors.CLUSTER_AUTHORIZATION_FAILED, epoch.partitionId)
        }
      }.asJava
      
      (topic, lastOffsetsByEpoch)
    }.asJava
  }
}