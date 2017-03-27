package unit.kafka.server.epoch.suites

import kafka.integration.FetcherTest
import kafka.log.{LogSegmentTest, LogTest, LogValidatorTest}
import kafka.message.MessageCompressionTest
import kafka.server.LogRecoveryTest
import kafka.server.epoch._
import org.apache.kafka.common.requests.RequestResponseTest
import org.junit.runner.RunWith
import org.junit.runners.Suite
import org.junit.runners.Suite.SuiteClasses
import unit.kafka.server.checkpoints.{LeaderEpochCheckpointFileTest, OffsetCheckpointFileTest}
import unit.kafka.server.epoch.OffsetsForLeaderEpochTest

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

/**
  * Set of tests relating to Epoch Functionality
  */

@RunWith(classOf[Suite])
@SuiteClasses(Array(
  //Epoch
  classOf[EpochDrivenReplicationProtocolAcceptanceTest],
  classOf[LeaderEpochIntegrationTest],
  classOf[OffsetsForLeaderEpochTest],
  classOf[OffsetsForLeaderEpochIntegrationTest],
  classOf[ReplicaFetcherThreadTest],
  classOf[LeaderEpochFetcherTest],
  classOf[LeaderEpochFileCacheTest],

  //Checkpoint
  classOf[LeaderEpochCheckpointFileTest],
  classOf[OffsetCheckpointFileTest],

  //Others
  classOf[FetcherTest],
  classOf[RequestResponseTest],
  classOf[LogSegmentTest],
  classOf[LogTest],
  classOf[LogValidatorTest],
  classOf[MessageCompressionTest],
  classOf[LogRecoveryTest],
  classOf[LogSegmentTest],
  classOf[LogValidatorTest]
))
class EpochTestSuite
