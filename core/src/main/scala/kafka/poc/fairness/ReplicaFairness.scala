package kafka.poc.fairness

import kafka.admin.BrokerMetadata
import kafka.poc.Replica

import scala.collection.{mutable, Seq}

class ReplicaFairness(brokersToReplicas: Seq[(BrokerMetadata, Seq[Replica])], allBrokers: Seq[BrokerMetadata]) extends Fairness {

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

  //Summarise the topology as BrokerMetadata -> ReplicaCount
  def brokerReplicaCounts() = mutable.LinkedHashMap(
    brokersToReplicas
      .map { case (x, y) => (x, y.size) }
      .sortBy(_._2)
      : _*
  )

  private def rackReplicaCounts() = mutable.LinkedHashMap(
    brokersToReplicas
      .map { case (x, y) => (x, y.size) }
      .groupBy(_._1.rack.get)
      .mapValues(_.map(_._2).sum)
      .toSeq
      .sortBy(_._2)
      : _*
  )

  //Define rackFairValue: floor(replica-count / rack-count) replicas
  def rackFairValue() = {
    Math.ceil(
      brokerReplicaCounts.values.sum.toFloat /
        rackCount
    ).toInt
  }

  def rackCount: Int = allBrokers.map(_.rack.get).distinct.size

  //Define  floor(replica-count / broker-count) replicas
  def brokerFairValue() = {
    Math.ceil(
      brokerReplicaCounts.values.sum.toFloat /
        allBrokers.size
    ).toInt
  }

  private def countFromPar(rack: String): Int = {
    Math.abs(rackReplicaCounts.get(rack).get - rackFairValue.toInt)
  }

  private def countFromPar(broker: BrokerMetadata): Int = {
    Math.abs(brokerReplicaCounts.get(broker).get - brokerFairValue.toInt)
  }
}


