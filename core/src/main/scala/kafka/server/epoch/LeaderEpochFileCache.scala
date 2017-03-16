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
  def assignToLeo(leaderEpoch: Int)
  def assign(leaderEpoch: Int, offset: Long)
  def latestEpoch(): Int
  def endOffsetFor(epoch: Int): Long
  def clearLatest(offset: Long, retainMatchingOffset: Boolean = true)
  def clearOldest(offset: Long, retainMatchingOffset: Boolean = true)
  def clear()
}

object Constants {
  val UNSUPPORTED_EPOCH_OFFSET = EpochEndOffset.UNDEFINED_OFFSET
  val UNSUPPORTED_EPOCH = -1
}

/**
  * Represents a cache of (LeaderEpoch => Offset) mappings derived from the log.
  * The cache contains all LeaderEpochs currently in a single log.
  * Offset is the offset of the first message in each epoch.
  *
  * @param leo a function that determines the log end offset
  * @param checkpoint the checkpoint file
  */
class LeaderEpochFileCache(leo: () => LogOffsetMetadata, checkpoint: LeaderEpochCheckpoint) extends LeaderEpochCache with Logging {
  private val lock = new ReentrantReadWriteLock()
  private[epoch] var epochs = lock synchronized { ListBuffer(checkpoint.read(): _*) }

  /**
    * Assigns the passed Leader Epoch to the current LEO
    * Once the epoch is assigned it cannot be reassigned
    *
    * @param leaderEpoch
    * @param offset
    */
  override def assignToLeo(epoch: Int) = {
    assign(epoch, leo().messageOffset)
  }

  /**
    * Assigns the passed Leader Epoch to the passed Offset
    * Once the epoch is assigned it cannot be reassigned
    *
    * @param leaderEpoch
    * @param offset
    */
  override def assign(epoch: Int, offset: Long): Unit = {
    inWriteLock(lock) {
      if (epoch >= 0 && epoch > latestEpoch()) {
        epochs += EpochEntry(epoch, offset)
        flush()
      }
    }
  }

  /**
    * Returns the current Leader Epoch
    *
    * @return
    */
  override def latestEpoch(): Int = {
    inReadLock(lock) {
      if (epochs.isEmpty) UNSUPPORTED_EPOCH else epochs.last.epoch
    }
  }

  /**
    * Returns the End Offset for a requested Leader Epoch.
    *
    * This is defined as the start offset of the first Leader Epoch larger than the
    * Leader Epoch requested, or else the Log End Offset if the latest epoch was requested.
    *
    * @param epoch
    * @return offset
    */
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

  /**
    * Removes all epoch entries from the store greater than the passed offset.
    * Can be inclusive or exclusive.
    *
    * @param offset
    * @param retainMatchingOffset if true the matching offset will be retained, else it will be removed
    */
  override def clearLatest(offset: Long, retainMatchingOffset: Boolean = true): Unit = {
    inWriteLock(lock) {
      if (offset >= 0 && offset <= latestOffset) {
        epochs = if(retainMatchingOffset)
          epochs.filter(entry => entry.startOffset <= offset)
        else
          epochs.filter(entry => entry.startOffset < offset)
        flush()
      }
    }
  }

  /**
    * Removes all epoch entries from the store less than the passed offset.
    * Can be inclusive or exclusive.
    *
    * @param offset
    * @param retainMatchingOffset the matching offset will be kept, else it will be removed
    */
  override def clearOldest(offset: Long, retainMatchingOffset: Boolean = true): Unit = {
    inWriteLock(lock) {
      if (offset >= 0 && offset >= earliestOffset) {
        epochs = if(retainMatchingOffset)
          epochs.filter(entry => entry.startOffset >= offset)
        else
          epochs.filter(entry => entry.startOffset > offset)
        flush()
      }
    }
  }

  /**
    * Delete all entries.
    */
  override def clear() = {
    inWriteLock(lock) {
      epochs.clear()
      flush()
    }
  }

  private def earliestOffset(): Long = {
    if (epochs.isEmpty) -1 else epochs.head.startOffset
  }

  private def latestOffset(): Long = {
    if (epochs.isEmpty) -1 else epochs.last.startOffset
  }

  private def flush(): Unit = {
    checkpoint.write(epochs)
  }

  def epochEntries(): ListBuffer[EpochEntry] ={
    epochs
  }
}

// Mapping of epoch to the first offset of the subsequent epoch
case class EpochEntry(epoch: Int, startOffset: Long)