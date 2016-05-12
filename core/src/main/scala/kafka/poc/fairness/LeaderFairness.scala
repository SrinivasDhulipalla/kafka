package kafka.poc.fairness

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc.TopologyHelper

import scala.collection.immutable.ListMap
import scala.collection.{Iterable, Map, Seq}

class LeaderFairness(brokersToLeaders: Seq[(BrokerMetadata, Iterable[TopicAndPartition])], allBrokers: Seq[BrokerMetadata]) extends Fairness with TopologyHelper {

  private val rackCount = allBrokers.map(_.rack.get).distinct.size
  private val rackLeaderCounts: Map[String, Int] = getRackLeaderCounts(brokersToLeaders)
  private val brokerLeaderCounts: ListMap[BrokerMetadata, Int] = getBrokerLeaderCounts(brokersToLeaders)
  val brokerFairValue = Math.ceil(leaderCount.toFloat / allBrokers.size).toInt
  val rackFairValue = Math.ceil(leaderCount.toFloat / rackCount).toInt

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
    brokerLeaderCounts
      .filter(_._2 > brokerFairValue)
      .keys.toSeq.distinct
  }

  def belowParBrokers(): Seq[BrokerMetadata] = {
    brokerLeaderCounts
      .filter(_._2 < brokerFairValue)
      .keys.toSeq.distinct
  }

  private def leaderCount(): Int = {
    brokersToLeaders
      .map { case (x, y) => y.size }
      .sum
  }

}
