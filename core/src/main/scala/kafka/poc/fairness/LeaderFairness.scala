package kafka.poc.fairness

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection.{Iterable, mutable, Map, Seq}

class LeaderFairness(brokersToLeaders: Seq[(BrokerMetadata, Iterable[TopicAndPartition])], allBrokers: Seq[BrokerMetadata]) extends Fairness {

  def aboveParRacks(): Seq[String] = {
    rackLeaderCounts
      .filter(_._2 > rackFairValue)
      .keys
      .toSeq
      .distinct
  }

  def belowParRacks(): Seq[String] = {
    rackLeaderCounts
      .filter(_._2 < rackFairValue)
      .keys
      .toSeq
      .distinct
  }

  def aboveParBrokers(): Seq[BrokerMetadata] = {
    val vals = brokerLeaderCounts
      .filter(_._2 > brokerFairValue)
      .keys.toSeq.distinct
    vals
  }

  def belowParBrokers(): Seq[BrokerMetadata] = {
    brokerLeaderCounts
      .filter(_._2 < brokerFairValue)
      .keys.toSeq.distinct
  }

  def brokerLeaderCounts() = mutable.LinkedHashMap(
    brokersToLeaders
      .map { case (x, y) => (x, y.size) }
      .sortBy(_._2)
      : _*
  )

  private def rackLeaderCounts: Map[String, Int] = {
    brokersToLeaders
      .map { case (x, y) => (x, y.size) }
      .groupBy(_._1.rack.get)
      .mapValues(_.map(_._2).sum)
  }

  private def leaderCount(): Int = {
    brokersToLeaders
      .map { case (x, y) => y.size }
      .sum
  }

  private def brokerCount(): Int = allBrokers.size

  private def rackCount: Int = allBrokers.map(_.rack.get).distinct.size

  def rackFairValue() = Math.ceil(leaderCount.toFloat / rackCount).toInt


  def brokerFairValue() = Math.ceil(leaderCount.toFloat / brokerCount).toInt

}
