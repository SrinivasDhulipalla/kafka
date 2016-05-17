package kafka.poc.fairness

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc.topology.TopologyHelper

import scala.collection.immutable.ListMap
import scala.collection.{Iterable, Map, Seq}

class LeaderFairness(brokersToLeaders: Seq[(BrokerMetadata, Iterable[TopicAndPartition])], allBrokers: Seq[BrokerMetadata]) extends Fairness with TopologyHelper {

  private val rackCount = allBrokers.map(_.rack.get).distinct.size
  private val rackLeaderCounts: Map[String, Int] = getRackLeaderCounts(brokersToLeaders)
  private val brokerLeaderCounts: ListMap[BrokerMetadata, Int] = getBrokerLeaderCounts(brokersToLeaders)
  val brokerFairValue:Float = leaderCount.toFloat / allBrokers.size
  val rackFairValue:Float = leaderCount.toFloat / rackCount

  override def aboveParRacks(): Seq[String] = {
    rackLeaderCounts
      .filter(_._2 >  Math.floor(rackFairValue).toInt)
      .keys
      .toSeq
      .distinct
  }

  override def belowParRacks(): Seq[String] = {
    rackLeaderCounts
      .filter(_._2 <  Math.ceil(rackFairValue).toInt)
      .keys
      .toSeq
      .distinct
  }

  override def aboveParBrokers(): Seq[BrokerMetadata] = {
    brokerLeaderCounts
      .filter(_._2 > Math.floor(brokerFairValue).toInt)
      .keys.toSeq.distinct
  }

  override def belowParBrokers(): Seq[BrokerMetadata] = {
    brokerLeaderCounts
      .filter(_._2 < Math.ceil(brokerFairValue).toInt)
      .keys.toSeq.distinct
  }

  private def leaderCount(): Int = {
    brokersToLeaders
      .map { case (x, y) => y.size }
      .sum
  }

}
