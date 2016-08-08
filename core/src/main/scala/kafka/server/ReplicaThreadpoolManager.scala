package kafka.server

import kafka.common.TopicAndPartition
import kafka.utils.Logging
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

import scala.collection.{Set, Map}

class ReplicaThreadpoolManager(brokerConfig: KafkaConfig, replicaMgr: ReplicaManager, metrics: Metrics, time: Time, threadNamePrefix: Option[String] = None, quotaManager: ClientQuotaManager) extends FetcherManager with Logging {
  val regular = new ReplicaFetcherManager(brokerConfig, replicaMgr, metrics, time, threadNamePrefix, quotaManager, false)
  val throttled = new ReplicaFetcherManager(brokerConfig, replicaMgr, metrics, time, threadNamePrefix, quotaManager, true)


  override def removeFetcherForPartitions(partitions: Set[TopicAndPartition]): Unit = {
    regular.removeFetcherForPartitions(partitions)
    throttled.removeFetcherForPartitions(partitions)
  }

  override def closeAllFetchers(): Unit = {
    regular.closeAllFetchers
    throttled.closeAllFetchers
  }

  override def shutdownIdleFetcherThreads(): Unit = {
    regular.shutdownIdleFetcherThreads
    throttled.shutdownIdleFetcherThreads
  }

  override def addFetcherForPartitions(partitionAndOffsets: Map[TopicAndPartition, BrokerAndInitialOffset]): Unit = {
    regular.addFetcherForPartitions(partitionAndOffsets)
    throttled.addFetcherForPartitions(partitionAndOffsets)
  }

  def shutdown() {
    info("shutting down")
    regular.closeAllFetchers()
    throttled.closeAllFetchers()
    info("shutdown completed")
  }
}
