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
package unit.kafka.server.checkpoints

import java.io.File
import java.util.regex.Pattern

import kafka.server.checkpoints.{OffsetCheckpoint, OffsetCheckpointFile}
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnitSuite

import scala.collection.Map

class OffsetCheckpointFileTest extends JUnitSuite  with Logging{


  @Test
  def shouldPersistOverwriteAndReloadFile(): Unit ={
    val file = File.createTempFile("temp-checkpoint-file", System.nanoTime().toString)
    file.deleteOnExit()

    val checkpoint = new OffsetCheckpointFile(file)

    //Given
    val offsets = Map(new TopicPartition("foo", 1) -> 5L, new TopicPartition("bar", 2) -> 10L)

    //When
    checkpoint.write(offsets)

    //Then
    assertEquals(offsets, checkpoint.read())

    //Given overwrite
    val offsets2 = Map(new TopicPartition("foo", 2) -> 15L, new TopicPartition("bar", 3) -> 20L)

    //When
    checkpoint.write(offsets2)

    //Then
    assertEquals(offsets2, checkpoint.read())
  }


  @Test
  def shouldExceptionIfMalformed(): Unit ={
    fail()
  }



}
