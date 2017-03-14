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

import kafka.cluster.Replica
import kafka.server.checkpoints.LeaderEpochCheckpoint

trait LeaderEpochs{
  def becomeLeader(leaderEpoch: Int)
  def appendEpoch(leaderEpoch: Int, offset: Long)
  def epoch(): Int
  def lastOffsetFor(epoch: Int): Long
}

class SavedLeaderEpochs(replica: Replica, checkpoint: LeaderEpochCheckpoint) extends LeaderEpochs {
  private var epochs = Seq[EpochEntry]()
  initialise()

  private def initialise() = {
    loadFromCheckpoint()
    if (epochs.size == 0)
      epochs = epochs :+ EpochEntry(0, 0)
  }

  def becomeLeader(leaderEpoch: Int) = {
    updateWithLeo(leaderEpoch)
  }

  def appendEpoch(leaderEpoch: Int, offset: Long): Unit = {
    if (leaderEpoch >= 0) {
      epochs = epochs :+ EpochEntry(leaderEpoch, offset)
      flush()
    }
  }

  def epoch(): Int = {
    epochs.last.epoch
  }

  /**
    * LastOffset will be the start offset of the first Leader Epoch larger than the Leader Epoch passed
    * in the request or the Log End Offset if the leader's current epoch is equal to the one requested
    *
    * @param epoch
    * @return
    */
  def lastOffsetFor(requestedEpoch: Int): Long = {

    if (requestedEpoch == epoch()) {
      replica.logEndOffset.messageOffset
    }
    else {
      val latestEpochs = epochs.filter(e => e.epoch > requestedEpoch)
      if (latestEpochs.isEmpty)
        epochs.last.startOffset
      else
        latestEpochs.head.startOffset
    }
  }

  private def flush(): Unit = {
    checkpoint.write(epochs)
  }

  private def loadFromCheckpoint(): Unit = {
    epochs = checkpoint.read()
  }

  private def updateWithLeo(leaderEpoch: Int) = {
    appendEpoch(leaderEpoch, replica.logEndOffset.messageOffset)
  }

  private def resetTo(leaderEpoch: Int, offset: Long): Unit = {
    //are there older offsets? if so delete all older offsets and take this one. flush file.
    if (epochs.last.startOffset > offset) {
      epochs = epochs.filter(entry => entry.startOffset > offset)
      flush()
    }
  }
}

case class EpochEntry(epoch: Int, startOffset: Long)