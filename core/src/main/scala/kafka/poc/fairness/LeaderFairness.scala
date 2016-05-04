package kafka.poc.fairness

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc.Replica

import scala.collection.{Iterable, mutable, Map, Seq}

class LeaderFairness(brokersToLeaders: () => Seq[(BrokerMetadata, Iterable[TopicAndPartition])], partitionCount: Int, brokerCount: Int, rackCount: Int) extends Fairness {

  def aboveParRacks(): Seq[String] = {
    rackLeaderPartitionCounts
      .filter(_._2 > rackFairLeaderValue)
      .keys
      .toSeq
      .distinct
  }

  def belowParRacks(): Seq[String] = {
    rackLeaderPartitionCounts
      .filter(_._2 < rackFairLeaderValue)
      .keys
      .toSeq
      .distinct
  }

  def aboveParBrokers(): Seq[BrokerMetadata] = {
    brokerLeaderPartitionCounts
      .filter(_._2 > brokerFairLeaderValue)
      .keys.toSeq.distinct
  }

  def belowParBrokers(): Seq[BrokerMetadata] = {
    brokerLeaderPartitionCounts
      .filter(_._2 < brokerFairLeaderValue)
      .keys.toSeq.distinct
  }

  private def brokerLeaderPartitionCounts() = mutable.LinkedHashMap(
    brokersToLeaders()
      .map { case (x, y) => (x, y.size) }
      .sortBy(_._2)
      : _*
  )

  private def rackLeaderPartitionCounts: Map[String, Int] = {
    brokersToLeaders()
      .map { case (x, y) => (x, y.size) }
      .groupBy(_._1.rack.get)
      .mapValues(_.map(_._2).sum)
  }

  private def rackFairLeaderValue() = {
    Math.floor(partitionCount / rackCount)
  }

  private def brokerFairLeaderValue() = {
    Math.floor(partitionCount / brokerCount)
  }
}
