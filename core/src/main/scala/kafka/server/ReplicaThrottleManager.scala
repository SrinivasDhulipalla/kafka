package kafka.server

import java.util.concurrent.ConcurrentHashMap

import kafka.common.TopicAndPartition
import kafka.utils.Logging
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, Set}
import scala.collection.mutable.HashMap

class ReplicaThrottleManager(brokerConfig: KafkaConfig, replicaMgr: ReplicaManager, metrics: Metrics, time: Time, threadNamePrefix: Option[String] = None, quotaManager: ClientQuotaManager) extends FetcherManager with Logging {
  private val allThrottledPartitions = new ConcurrentHashMap[String, Seq[Int]]()
  private val regular = new ReplicaFetcherManager(brokerConfig, replicaMgr, metrics, time, threadNamePrefix, null, "ReplicaFetcherManager on broker " + brokerConfig.brokerId, "Replica", 0)
  private val throttled = new ReplicaFetcherManager(brokerConfig, replicaMgr, metrics, time, threadNamePrefix, quotaManager, "ThrottledReplicaFetcherManager on broker " + brokerConfig.brokerId, "ThrottledReplica", 1000)
  this.logIdent = "[ReplicaThrottleManager on broker " + brokerConfig.brokerId + "] "

  def isThrottled(tp: TopicAndPartition): Boolean =
    allThrottledPartitions.containsKey(tp.topic) && allThrottledPartitions.get(tp.topic).contains(tp.partition)

  override def removeFetcherForPartitions(partitions: Set[TopicAndPartition]): Map[TopicAndPartition, BrokerAndInitialOffset] = {
    val allRemoved = new scala.collection.mutable.HashMap[TopicAndPartition, BrokerAndInitialOffset]
    def throttledPartitions = partitions.filter(tp => isThrottled(tp))
    def notThrottledPartitions = partitions.filter(tp => !isThrottled(tp))

    regular.removeFetcherForPartitions(notThrottledPartitions).foreach { case (k, v) => allRemoved.put(k, v) }
    throttled.removeFetcherForPartitions(throttledPartitions).foreach { case (k, v) => allRemoved.put(k, v) }
    allRemoved
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
    def throttledPartitions = partitionAndOffsets.filter(tpo => isThrottled(tpo._1))
    def notThrottledPartitions = partitionAndOffsets.filter(tpo => !isThrottled(tpo._1))

    logger.info("Adding fetchers for throttled partitions: " + throttledPartitions.keySet.map(_.toString()))
    logger.info("Adding fetchers for regular partitions: " + notThrottledPartitions.keySet.map(_.toString()))

    regular.addFetcherForPartitions(notThrottledPartitions)
    throttled.addFetcherForPartitions(throttledPartitions)
  }

  def shutdown() {
    regular.shutdown()
    throttled.shutdown()
  }

  def updateThrottledPartitions(topic: String, partitions: Seq[Int]) = {
    allThrottledPartitions.put(topic, partitions)

    val allRemoved = new HashMap[TopicAndPartition, BrokerAndInitialOffset]

    //remove all partitions from throttled threads
    throttled.removeFetcherForAllPartitions()
      .foreach { case (k, v) =>
        allRemoved.put(k, v)
        logger.trace(s"Partition $k was removed from throttled fetcher pool")
      }

    //remove these partitions from regular threads
    regular.removeFetcherForPartitions(partitions.map(new TopicAndPartition(topic, _)).toSet)
      .foreach { case (k, v) =>
        allRemoved.put(k, v)
        logger.trace(s"Partition $k was removed from regular fetcher pool")
      }

    //add the partitions we removed to the throttled threads (we assume here that all partitions are present).
    val newlyThrottledPartitions = allRemoved.filter { case (k, v) => partitions.contains(k.partition) }
    logger.info("Moved partitions to throttled fetcher threads " + newlyThrottledPartitions.keySet.map(_.toString()))
    throttled.addFetcherForPartitions(newlyThrottledPartitions)
  }
}
