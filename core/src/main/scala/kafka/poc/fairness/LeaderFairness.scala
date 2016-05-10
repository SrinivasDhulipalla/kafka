package kafka.poc.fairness

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection.{Iterable, mutable, Map, Seq}

class LeaderFairness(brokersToLeaders: Seq[(BrokerMetadata, Iterable[TopicAndPartition])]) extends Fairness {

  def aboveParRacks(): Seq[String] = {
    rackLeaderCounts
      .filter(_._2 > rackFairLeaderValue)
      .keys
      .toSeq
      .distinct
  }

  def belowParRacks(): Seq[String] = {
    rackLeaderCounts
      .filter(_._2 < rackFairLeaderValue)
      .keys
      .toSeq
      .distinct
  }

  def aboveParBrokers(): Seq[BrokerMetadata] = {
    val vals = brokerLeaderCounts
      .filter(_._2 > brokerFairLeaderValue)
      .keys.toSeq.distinct
    vals
  }

  def belowParBrokers(): Seq[BrokerMetadata] = {
    brokerLeaderCounts
      .filter(_._2 < brokerFairLeaderValue)
      .keys.toSeq.distinct
  }

  private def brokerLeaderCounts() = mutable.LinkedHashMap(
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

  private def brokerCount(): Int = {
    brokersToLeaders.map(_._1).distinct.size
  }

  private def rackCount(): Int = {
    brokersToLeaders.map(_._1.rack.get).distinct.size
  }


  private def rackFairLeaderValue() = {
    Math.floor(leaderCount / rackCount)
  }

  private def brokerFairLeaderValue() = {
    Math.floor(leaderCount / brokerCount)
  }
}
