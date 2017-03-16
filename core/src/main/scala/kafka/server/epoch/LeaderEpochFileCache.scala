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

import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.server.LogOffsetMetadata
import kafka.server.checkpoints.LeaderEpochCheckpoint
import kafka.server.epoch.Constants.{UNSUPPORTED_EPOCH, UNSUPPORTED_EPOCH_OFFSET}
import kafka.utils.CoreUtils._
import kafka.utils.{Logging}
import org.apache.kafka.common.requests.EpochEndOffset

import scala.collection.mutable.ListBuffer

trait LeaderEpochCache {
  /**
    * Updates the epoch store with new epochs and the log end offset
    *
    * @param leaderEpoch
    * @param offset
    */
  def maybeUpdate(leaderEpoch: Int)

  /**
    * Updates the epoch store with new epochs and offset
    *
    * @param leaderEpoch
    * @param offset
    */
  def maybeUpdate(leaderEpoch: Int, offset: Long)

  /**
    * Returns the current epoch for this replica
    *
    * @return
    */
  def latestEpoch(): Int

  /**
    * Returns the start offset of the first Leader Epoch larger than the Leader Epoch passed
    * or the Log End Offset, if the leader's current epoch is equal to the one passed
    *
    * @param epoch
    * @return offset
    */
  def endOffsetFor(epoch: Int): Long

  /**
    * Remove all epoch entries from the store where startOffset < offset passed
    * This matches the logic in Log.truncateTo()
    *
    * @param leaderEpoch
    * @param offset
    */
  def resetTo(offset: Long)
}

object Constants {
  val UNSUPPORTED_EPOCH_OFFSET = EpochEndOffset.UNDEFINED_OFFSET
  val UNSUPPORTED_EPOCH = -1
}

class LeaderEpochFileCache(leo: () => LogOffsetMetadata, checkpoint: LeaderEpochCheckpoint) extends LeaderEpochCache with Logging {
  private val lock = new ReentrantReadWriteLock()
  private[epoch] var epochs = lock synchronized {ListBuffer(checkpoint.read(): _*)}


  override def maybeUpdate(epoch: Int) = {
    maybeUpdate(epoch, leo().messageOffset)
  }

  override def maybeUpdate(epoch: Int, offset: Long): Unit = {
    inWriteLock(lock) {
      if (epoch >= 0 && epoch > latestEpoch()) {
        epochs += EpochEntry(epoch, offset)
        flush()
      }
    }
  }

  override def latestEpoch(): Int = {
    inReadLock(lock) {
      if (epochs.isEmpty) UNSUPPORTED_EPOCH else epochs.last.epoch
    }
  }

  override def endOffsetFor(requestedEpoch: Int): Long = {
    inReadLock(lock) {
      //Use LEO if current epoch
      if (requestedEpoch == latestEpoch) {
        leo().messageOffset
      }
      else {
        //Use the start offset of the first subsequent epoch otherwise
        val subsequentEpochs = epochs.filter(e => e.epoch > requestedEpoch)
        if (subsequentEpochs.isEmpty)
          UNSUPPORTED_EPOCH_OFFSET
        else
          subsequentEpochs.head.startOffset
      }
    }
  }

  override def resetTo(offset: Long): Unit = {
    inWriteLock(lock) {
      if (offset >= latestEpoch) {
        epochs = epochs.filter(entry => entry.startOffset < offset)
        flush()
      }
    }
  }

  private def flush(): Unit = {
    checkpoint.write(epochs)
  }

}

// Mapping of epoch to the first offset of the subsequent epoch
case class EpochEntry(epoch: Int, startOffset: Long)