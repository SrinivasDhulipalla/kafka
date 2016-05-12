package kafka.poc.fairness

import kafka.admin.BrokerMetadata
import kafka.poc.{TopologyHelper, Replica}

import scala.collection.{mutable, Seq}

class ReplicaFairness(brokersToReplicas: Seq[(BrokerMetadata, Seq[Replica])], allBrokers: Seq[BrokerMetadata]) extends Fairness with TopologyHelper {

  private val rackReplicaCounts = getRackReplicaCounts(brokersToReplicas)
  private val brokerReplicaCounts = getBrokerReplicaCounts(brokersToReplicas)
  private val rackCount: Int = allBrokers.map(_.rack.get).distinct.size
  private val replicaCount: Float = brokerReplicaCounts.values.sum.toFloat
  val rackFairValue = Math.ceil(replicaCount / rackCount).toInt
  val brokerFairValue = Math.ceil(replicaCount / allBrokers.size).toInt


  def aboveParRacks(): Seq[String] = {
    //return racks for brokers where replica count is over fair value
    rackReplicaCounts
      .filter { x => x._2.toInt > rackFairValue }
      .keys
      .toSeq
      .distinct
  }

  def belowParRacks(): Seq[String] = {
    //return racks for brokers where replica count is over fair value
    rackReplicaCounts
      .filter(_._2.toInt < rackFairValue)
      .keys
      .toSeq
      .distinct
  }

  def aboveParBrokers(): Seq[BrokerMetadata] = {
    //return brokers where replica count is over fair value
    brokerReplicaCounts
      .filter(_._2.toInt > brokerFairValue)
      .keys.toSeq.distinct
  }

  def belowParBrokers(): Seq[BrokerMetadata] = {
    brokerReplicaCounts
      .filter(_._2 < brokerFairValue)
      .keys.toSeq.distinct
  }

  private def countFromPar(rack: String): Int = {
    Math.abs(rackReplicaCounts.get(rack).get - rackFairValue)
  }

  private def countFromPar(broker: BrokerMetadata): Int = {
    Math.abs(brokerReplicaCounts.get(broker).get - brokerFairValue)
  }
}


