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

package kafka.server

import java.util

import kafka.admin.AdminUtils
import kafka.api.{KAFKA_0_10_0_IV0, KAFKA_0_10_1_IV1, KAFKA_0_10_1_IV2, KAFKA_0_9_0}
import kafka.cluster.BrokerEndPoint
import kafka.common.KafkaStorageException
import kafka.utils.Exit
import org.apache.kafka.common.internals.FatalExitError
import kafka.log.LogConfig
import kafka.server.ReplicaFetcherThread._
import kafka.server.epoch.{LeaderEpochFetcher, LeaderEpochCache, PartitionEpoch}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.{EpochEndOffset, FetchResponse, ListOffsetRequest, ListOffsetResponse, OffsetForLeaderEpochResponse, FetchRequest => JFetchRequest}
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection.{Map, Set}

class ReplicaFetcherThread(name: String,
                           fetcherId: Int,
                           sourceBroker: BrokerEndPoint,
                           brokerConfig: KafkaConfig,
                           replicaMgr: ReplicaManager,
                           metrics: Metrics,
                           time: Time,
                           quota: ReplicationQuotaManager,
                           blockingSend: Option[BlockingSend] = None)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                sourceBroker = sourceBroker,
                                fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
                                includePartitionInitialisation = true,
                                isInterruptible = false) {

  type REQ = FetchRequest
  type PD = PartitionData

  val network = blockingSend
    .getOrElse(new ReplicaFetcherBlockingSend(sourceBroker, brokerConfig, metrics, time, fetcherId, clientId))

  private val fetchRequestVersion: Short =
    if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV1) 3
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_0_IV0) 2
    else if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
    else 0
  private val socketTimeout: Int = brokerConfig.replicaSocketTimeoutMs
  private val replicaId = brokerConfig.brokerId
  private val maxWait = brokerConfig.replicaFetchWaitMaxMs
  private val minBytes = brokerConfig.replicaFetchMinBytes
  private val maxBytes = brokerConfig.replicaFetchResponseMaxBytes
  private val fetchSize = brokerConfig.replicaFetchMaxBytes

  private def clientId = name

  override def shutdown(): Unit = {
    super.shutdown()
    network.close()
  }

  // process fetched data
  def processPartitionData(topicPartition: TopicPartition, fetchOffset: Long, partitionData: PartitionData) {
    try {
      val replica = replicaMgr.getReplica(topicPartition).get
      val records = partitionData.toRecords

      maybeWarnIfOversizedRecords(records, topicPartition)

      if (fetchOffset != replica.logEndOffset.messageOffset)
        throw new RuntimeException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(topicPartition, fetchOffset, replica.logEndOffset.messageOffset))
      if (logger.isTraceEnabled)
        trace("Follower %d has replica log end offset %d for partition %s. Received %d messages and leader hw %d"
          .format(replica.brokerId, replica.logEndOffset.messageOffset, topicPartition, records.sizeInBytes, partitionData.highWatermark))

      replica.log.get.append(records, assignOffsets = false)

      if (logger.isTraceEnabled)
        trace("Follower %d has replica log end offset %d after appending %d bytes of messages for partition %s"
          .format(replica.brokerId, replica.logEndOffset.messageOffset, records.sizeInBytes, topicPartition))
      val followerHighWatermark = replica.logEndOffset.messageOffset.min(partitionData.highWatermark)
      // for the follower replica, we do not need to keep
      // its segment base offset the physical position,
      // these values will be computed upon making the leader
      replica.highWatermark = new LogOffsetMetadata(followerHighWatermark)
      if (logger.isTraceEnabled)
        trace(s"Follower ${replica.brokerId} set replica high watermark for partition $topicPartition to $followerHighWatermark")
      if (quota.isThrottled(topicPartition))
        quota.record(records.sizeInBytes)
    } catch {
      case e: KafkaStorageException =>
        fatal(s"Disk error while replicating data for $topicPartition", e)
        Exit.halt(1)
    }
  }

  def maybeWarnIfOversizedRecords(records: MemoryRecords, topicPartition: TopicPartition): Unit = {
    // oversized messages don't cause replication to fail from fetch request version 3 (KIP-74)
    if (fetchRequestVersion <= 2 && records.sizeInBytes > 0 && records.validBytes <= 0)
      error(s"Replication is failing due to a message that is greater than replica.fetch.max.bytes for partition $topicPartition. " +
        "This generally occurs when the max.message.bytes has been overridden to exceed this value and a suitably large " +
        "message has also been sent. To fix this problem increase replica.fetch.max.bytes in your broker config to be " +
        "equal or larger than your settings for max.message.bytes, both at a broker and topic level.")
  }

  /**
   * Handle a partition whose offset is out of range and return a new fetch offset.
   */
  def handleOffsetOutOfRange(topicPartition: TopicPartition): Long = {
    val replica = replicaMgr.getReplica(topicPartition).get

    /**
     * Unclean leader election: A follower goes down, in the meanwhile the leader keeps appending messages. The follower comes back up
     * and before it has completely caught up with the leader's logs, all replicas in the ISR go down. The follower is now uncleanly
     * elected as the new leader, and it starts appending messages from the client. The old leader comes back up, becomes a follower
     * and it may discover that the current leader's end offset is behind its own end offset.
     *
     * In such a case, truncate the current follower's log to the current leader's end offset and continue fetching.
     *
     * There is a potential for a mismatch between the logs of the two replicas here. We don't fix this mismatch as of now.
     */
    val leaderEndOffset: Long = earliestOrLatestOffset(topicPartition, ListOffsetRequest.LATEST_TIMESTAMP,
      brokerConfig.brokerId)

    if (leaderEndOffset < replica.logEndOffset.messageOffset) {
      // Prior to truncating the follower's log, ensure that doing so is not disallowed by the configuration for unclean leader election.
      // This situation could only happen if the unclean election configuration for a topic changes while a replica is down. Otherwise,
      // we should never encounter this situation since a non-ISR leader cannot be elected if disallowed by the broker configuration.
      if (!LogConfig.fromProps(brokerConfig.originals, AdminUtils.fetchEntityConfig(replicaMgr.zkUtils,
        ConfigType.Topic, topicPartition.topic)).uncleanLeaderElectionEnable) {
        // Log a fatal error and shutdown the broker to ensure that data loss does not occur unexpectedly.
        fatal(s"Exiting because log truncation is not allowed for partition $topicPartition, current leader " +
          s"${sourceBroker.id}'s latest offset $leaderEndOffset is less than replica ${brokerConfig.brokerId}'s latest " +
          s"offset ${replica.logEndOffset.messageOffset}")
        throw new FatalExitError
      }

      warn("Replica %d for partition %s reset its fetch offset from %d to current leader %d's latest offset %d"
        .format(brokerConfig.brokerId, topicPartition, replica.logEndOffset.messageOffset, sourceBroker.id, leaderEndOffset))
      replicaMgr.logManager.truncateTo(Map(topicPartition -> leaderEndOffset))
      leaderEndOffset
    } else {
      /**
       * If the leader's log end offset is greater than the follower's log end offset, there are two possibilities:
       * 1. The follower could have been down for a long time and when it starts up, its end offset could be smaller than the leader's
       * start offset because the leader has deleted old logs (log.logEndOffset < leaderStartOffset).
       * 2. When unclean leader election occurs, it is possible that the old leader's high watermark is greater than
       * the new leader's log end offset. So when the old leader truncates its offset to its high watermark and starts
       * to fetch from the new leader, an OffsetOutOfRangeException will be thrown. After that some more messages are
       * produced to the new leader. While the old leader is trying to handle the OffsetOutOfRangeException and query
       * the log end offset of the new leader, the new leader's log end offset becomes higher than the follower's log end offset.
       *
       * In the first case, the follower's current log end offset is smaller than the leader's log start offset. So the
       * follower should truncate all its logs, roll out a new segment and start to fetch from the current leader's log
       * start offset.
       * In the second case, the follower should just keep the current log segments and retry the fetch. In the second
       * case, there will be some inconsistency of data between old and new leader. We are not solving it here.
       * If users want to have strong consistency guarantees, appropriate configurations needs to be set for both
       * brokers and producers.
       *
       * Putting the two cases together, the follower should fetch from the higher one of its replica log end offset
       * and the current leader's log start offset.
       *
       */
      val leaderStartOffset: Long = earliestOrLatestOffset(topicPartition, ListOffsetRequest.EARLIEST_TIMESTAMP,
        brokerConfig.brokerId)
      warn("Replica %d for partition %s reset its fetch offset from %d to current leader %d's start offset %d"
        .format(brokerConfig.brokerId, topicPartition, replica.logEndOffset.messageOffset, sourceBroker.id, leaderStartOffset))
      val offsetToFetch = Math.max(leaderStartOffset, replica.logEndOffset.messageOffset)
      // Only truncate log when current leader's log start offset is greater than follower's log end offset.
      if (leaderStartOffset > replica.logEndOffset.messageOffset)
        replicaMgr.logManager.truncateFullyAndStartAt(topicPartition, leaderStartOffset)
      offsetToFetch
    }
  }

  // any logic for partitions whose leader has changed
  def handlePartitionsWithErrors(partitions: Iterable[TopicPartition]) {
    delayPartitions(partitions, brokerConfig.replicaFetchBackoffMs.toLong)
  }

  protected def fetch(fetchRequest: FetchRequest): Seq[(TopicPartition, PartitionData)] = {
    val clientResponse = network.sendRequest(fetchRequest.underlying)
    val fetchResponse = clientResponse.responseBody.asInstanceOf[FetchResponse]
    fetchResponse.responseData.asScala.toSeq.map { case (key, value) =>
      key -> new PartitionData(value)
    }
  }

  private def earliestOrLatestOffset(topicPartition: TopicPartition, earliestOrLatest: Long, consumerId: Int): Long = {
    val requestBuilder = if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2) {
        val partitions = Map(topicPartition -> (earliestOrLatest: java.lang.Long))
        ListOffsetRequest.Builder.forReplica(1, replicaId).setTargetTimes(partitions.asJava)
      } else {
        val partitions = Map(topicPartition -> new ListOffsetRequest.PartitionData(earliestOrLatest, 1))
        ListOffsetRequest.Builder.forReplica(0, replicaId).setOffsetData(partitions.asJava)
      }
    val clientResponse = network.sendRequest(requestBuilder)
    val response = clientResponse.responseBody.asInstanceOf[ListOffsetResponse]
    val partitionData = response.responseData.get(topicPartition)
    partitionData.error match {
      case Errors.NONE =>
        if (brokerConfig.interBrokerProtocolVersion >= KAFKA_0_10_1_IV2)
          partitionData.offset
        else
          partitionData.offsets.get(0)
      case error => throw error.exception
    }
  }

  private def epochCache(tp: TopicPartition): LeaderEpochCache =  replicaMgr.getReplica(tp).get.epochs.get

  override protected def initialisePartitions(allPartitions: Seq[(TopicPartition, PartitionFetchState)]) = {
    val partitions = allPartitions.filter { case (_, state) => state.isInitialising }.toMap
    val epochRequests = partitions.map { case (tp, state) => PartitionEpoch(tp, epochCache(tp).latestEpoch) }.toSet

    if (!partitions.isEmpty) {
      val leaderEpochs = fetchEpochsFromLeader(epochRequests)
      val truncationPoints = truncate(leaderEpochs)
      initialisationComplete(partitions.keySet.toSeq, truncationPoints)
    }
  }

  protected def buildFetchRequest(partitionMap: Seq[(TopicPartition, PartitionFetchState)]): FetchRequest = {
    val requestMap = new util.LinkedHashMap[TopicPartition, JFetchRequest.PartitionData]

    partitionMap.foreach { case (topicPartition, partitionFetchState) =>
      if (partitionFetchState.isActive && !shouldFollowerThrottle(quota, topicPartition))
        requestMap.put(topicPartition, new JFetchRequest.PartitionData(partitionFetchState.offset, fetchSize))
    }

    val requestBuilder = JFetchRequest.Builder.forReplica(fetchRequestVersion, replicaId, maxWait, minBytes, requestMap)
      .setMaxBytes(maxBytes)
    new FetchRequest(requestBuilder)
  }

  /**
    * if there is an error we truncate to the HWM
    * if the leader's offset is greater, we stick with the Log End Offset
    * otherwise we truncate to the leaders offset.
    */
  def truncate(epochOffsets: Map[TopicPartition, EpochEndOffset]): Map[TopicPartition, Long] = {
    val truncationPoints = epochOffsets
      .map { case (tp, epochOffset) =>
        val replica = replicaMgr.getReplica(tp).get
        val truncateTo =
          if (epochOffset.hasError)
            replica.highWatermark.messageOffset
          else if (epochOffset.endOffset() > replica.logEndOffset.messageOffset)
            replica.logEndOffset.messageOffset
          else
            epochOffset.endOffset
        (tp, truncateTo)
      }.toMap

    replicaMgr.logManager.truncateTo(truncationPoints)
    truncationPoints
  }

  /**
   *  To avoid ISR thrashing, we only throttle a replica on the follower if it's in the throttled replica list,
   *  the quota is exceeded and the replica is not in sync.
   */
  private def shouldFollowerThrottle(quota: ReplicaQuota, topicPartition: TopicPartition): Boolean = {
    val isReplicaInSync = fetcherLagStats.isReplicaInSync(topicPartition.topic, topicPartition.partition)
    quota.isThrottled(topicPartition) && quota.isQuotaExceeded && !isReplicaInSync
  }

  def fetchEpochsFromLeader(partitions: Set[PartitionEpoch]): Map[TopicPartition, EpochEndOffset] = {
    new LeaderEpochFetcher(network).leaderOffsetsFor(partitions)
  }
}

object ReplicaFetcherThread {

  private[server] class FetchRequest(val underlying: JFetchRequest.Builder) extends AbstractFetcherThread.FetchRequest {
    def isEmpty: Boolean = underlying.fetchData().isEmpty
    def offset(topicPartition: TopicPartition): Long =
      underlying.fetchData().asScala(topicPartition).offset
  }

  private[server] class PartitionData(val underlying: FetchResponse.PartitionData) extends AbstractFetcherThread.PartitionData {

    def error = underlying.error

    def toRecords: MemoryRecords = {
      underlying.records.asInstanceOf[MemoryRecords]
    }

    def highWatermark: Long = underlying.highWatermark

    def exception: Option[Throwable] = error match {
      case Errors.NONE => None
      case e => Some(e.exception)
    }
  }
}