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

import kafka.server.LogOffsetMetadata
import kafka.server.checkpoints.LeaderEpochCheckpoint
import kafka.server.epoch.Constants.{UNSUPPORTED_EPOCH, UNSUPPORTED_EPOCH_OFFSET}
import kafka.utils.Logging

import scala.collection.mutable.ListBuffer

trait LeaderEpochs {

  def maybeUpdate(leaderEpoch: Int)

  def maybeUpdate(leaderEpoch: Int, offset: Long)

  def latestEpoch(): Int

  def endOffsetFor(epoch: Int): Long
}

object Constants {
  val UNSUPPORTED_EPOCH_OFFSET = -1
  val UNSUPPORTED_EPOCH = -1
}

class LeaderEpochCache(leo: () => LogOffsetMetadata, checkpoint: LeaderEpochCheckpoint) extends LeaderEpochs with Logging {
  private[epoch] var epochs = ListBuffer(checkpoint.read(): _*)

  def maybeUpdate(epoch: Int) = {
    maybeUpdate(epoch, leo().messageOffset)
  }

  def maybeUpdate(epoch: Int, offset: Long): Unit = {
    if (epoch >= 0 && epoch > latestEpoch()) {
      epochs += EpochEntry(epoch, offset)
      flush()
    }
  }

  def latestEpoch(): Int = if(epochs.isEmpty) UNSUPPORTED_EPOCH else epochs.last.epoch

  /**
    * LastOffset will be the start offset of the first Leader Epoch larger than the Leader Epoch passed
    * in the request or the Log End Offset if the leader's current epoch is equal to the one requested
    *
    * @param epoch
    * @return
    */
  def endOffsetFor(requestedEpoch: Int): Long = {
    //Use LEO if requested current epoch
    if (requestedEpoch == latestEpoch()) {
      leo().messageOffset
    }
    else {
      //Return the start offset of the first subsequent epoch
      val subsequentEpochs = epochs.filter(e => e.epoch > requestedEpoch)
      if (subsequentEpochs.isEmpty)
        UNSUPPORTED_EPOCH_OFFSET
      else
        subsequentEpochs.head.startOffset
    }
  }

  private def flush(): Unit = {
    checkpoint.write(epochs)
  }

  //TODO we will need this later...
  private def resetTo(leaderEpoch: Int, offset: Long): Unit = {
    //are there older offsets? if so delete all older offsets and take this one. flush file.
    if (epochs.last.startOffset > offset) {
      epochs = epochs.filter(entry => entry.startOffset > offset)
      flush()
    }
  }
}

// Mapping of epoch to the first offset of the subsequent epoch
case class EpochEntry(epoch: Int, startOffset: Long)