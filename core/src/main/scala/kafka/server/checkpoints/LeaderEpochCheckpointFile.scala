/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server.checkpoints

import java.io._
import java.nio.file.{FileSystems, Paths}
import java.util.regex.Pattern

import kafka.server.epoch.EpochEntry
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

import scala.collection._

private object LeaderEpochCheckpointFile {
  private val WhiteSpacesPattern = Pattern.compile("\\s+")
  private val CurrentVersion = 0
}

trait LeaderEpochCheckpoint {
  def write(epochs: Seq[EpochEntry])
  def read(): Seq[EpochEntry]
}

object LeaderEpochFile {
  private val LeaderEpochCheckpointFilename = "leader-epoch-checkpoint"
  def newFile(dir: File) = {new File(dir, LeaderEpochCheckpointFilename)}
}

/**
  * This class saves out a map of LeaderEpoch=>offsets to a file for a certain replica
  */
class LeaderEpochCheckpointFile(val file: File) extends CheckpointFileFormatter[EpochEntry] with LeaderEpochCheckpoint {
  val checkpoint = new CheckpointFile[EpochEntry](file, OffsetCheckpoint.CurrentVersion, this)

  override def toLine(entry: EpochEntry): String = {
    s"${entry.epoch} ${entry.startOffset}"
  }

  override def fromLine(line: String): Option[EpochEntry] = {
    OffsetCheckpoint.WhiteSpacesPattern.split(line) match {
      case Array(epoch, offset) =>
        Some(EpochEntry(epoch.toInt, offset.toLong))
      case _ => None
    }
  }

  def write(epochs: Seq[EpochEntry]) = {
    checkpoint.write(epochs)
  }

  def read(): Seq[EpochEntry] = {
    checkpoint.read()
  }
}
