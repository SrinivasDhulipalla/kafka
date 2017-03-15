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

import org.apache.kafka.common.record.ByteBufferLogInputStream.ByteBufferLogEntry

/**
  * This is called on every inbound message
  */
trait MessageAppendInterceptor {
  def onMessage(entry: ByteBufferLogEntry)
}

/**
  * Used by the leader to set the epoch on the message
  * as they come in from clients.
  */
class EpochSettingInterceptor(epoch: Int) extends MessageAppendInterceptor {
  def onMessage(entry: ByteBufferLogEntry) = {
    entry.setLeaderEpoch(epoch)
  }
}

/**
  * Used by the follower to track changes in the leader epoch
  * as messages are processed.
  */
class EpochTrackingInterceptor(epochs: LeaderEpochs) extends MessageAppendInterceptor {
  override def onMessage(entry: ByteBufferLogEntry): Unit = {
    //Check to see if epoch has changed, record it if it has
    val epochOnReplicatedMessage = entry.record.leaderEpoch()
    if (epochOnReplicatedMessage > epochs.latestEpoch())
      epochs.maybeUpdate(epochOnReplicatedMessage, entry.offset())
  }
}

object EmptyEpochInterceptor extends MessageAppendInterceptor {
  override def onMessage(entry: ByteBufferLogEntry): Unit = {}
}

