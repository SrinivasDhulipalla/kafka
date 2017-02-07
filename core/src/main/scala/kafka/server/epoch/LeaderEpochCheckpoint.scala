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
package kafka.server.epoch

import java.io._
import java.nio.file.{FileSystems, Paths}
import java.util.regex.Pattern

import kafka.utils.Logging
import org.apache.kafka.common.utils.Utils

import scala.collection._

object LeaderEpochCheckpoint {
  private val WhiteSpacesPattern = Pattern.compile("\\s+")
  private val CurrentVersion = 0
}

/**
 * This class saves out a map of LeaderEpoch=>offsets to a file for a certain replica
  *
  * TODO we could probably merge this with the offsets checkpoint file
  *
  * TODO we should probably be appending to the file rather than always recreating it.
 */
class LeaderEpochCheckpoint(val file: File) extends Logging {
  import LeaderEpochCheckpoint._
  private val path = file.toPath.toAbsolutePath
  private val tempPath = Paths.get(path.toString + ".tmp")
  private val lock = new Object()
  file.createNewFile() // in case the file doesn't exist

  def write(epochs: Seq[EpochEntry]) {
    lock synchronized {
      // write to temp file and then swap with the existing file
      val fileOutputStream = new FileOutputStream(tempPath.toFile)
      val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream))
      try {
        writer.write(CurrentVersion.toString)
        writer.newLine()

        writer.write(epochs.size.toString)
        writer.newLine()

        epochs.foreach { entry =>
          writer.write(s"${entry.epoch} ${entry.startOffset}")
          writer.newLine()
        }

        writer.flush()
        fileOutputStream.getFD().sync()
      } catch {
        case e: FileNotFoundException =>
          if (FileSystems.getDefault.isReadOnly) {
            fatal("Halting writes to offset checkpoint file because the underlying file system is inaccessible : ", e)
            Runtime.getRuntime.halt(1)
          }
          throw e
      } finally {
        writer.close()
      }

      Utils.atomicMoveWithFallback(tempPath, path)
    }
  }

  def read(): Seq[EpochEntry] = {

    def malformedLineException(line: String) =
      new IOException(s"Malformed line in leader epoch offset checkpoint file: $line'")

    lock synchronized {
      val reader = new BufferedReader(new FileReader(file))
      var line: String = null
      try {
        line = reader.readLine()
        if (line == null)
          return Seq.empty
        val version = line.toInt
        version match {
          case CurrentVersion =>
            line = reader.readLine()
            if (line == null)
              return Seq.empty
            val expectedSize = line.toInt
            val offsets = mutable.ListBuffer[EpochEntry]()
            line = reader.readLine()
            while (line != null) {
              WhiteSpacesPattern.split(line) match {
                case Array(epoch, offset) =>
                  offsets += EpochEntry(epoch.toInt, offset.toLong)
                  line = reader.readLine()
                case _ => throw malformedLineException(line)
              }
            }
            if (offsets.size != expectedSize)
              throw new IOException(s"Expected $expectedSize entries but found only ${offsets.size}")
            offsets.toSeq
          case _ =>
            throw new IOException("Unrecognized version of the highwatermark checkpoint file: " + version)
        }
      } catch {
        case _: NumberFormatException => throw malformedLineException(line)
      } finally {
        reader.close()
      }
    }
  }
  
}
