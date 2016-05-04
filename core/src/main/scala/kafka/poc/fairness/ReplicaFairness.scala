package kafka.poc.fairness

import kafka.admin.BrokerMetadata
import kafka.poc.Replica

import scala.collection.{mutable, Seq}

class ReplicaFairness(brokersToReplicas: () => Seq[(BrokerMetadata, Seq[Replica])], rackCount: Int) extends Fairness {

  def aboveParRacks(): Seq[String] = {
    //return racks for brokers where replica count is over fair value
    rackReplicaCounts
      .filter(_._2 > rackFairReplicaValue)
      .keys
      .toSeq.distinct
  }

  def belowParRacks(): Seq[String] = {
    //return racks for brokers where replica count is over fair value
    rackReplicaCounts
      .filter(_._2 < rackFairReplicaValue)
      .keys
      .toSeq
      .distinct
  }

  def aboveParBrokers(): Seq[BrokerMetadata] = {
    //return racks for brokers where replica count is over fair value
    brokerReplicaCounts
      .filter(_._2 > brokerFairReplicaValue)
      .keys.toSeq.distinct
  }

  def belowParBrokers(): Seq[BrokerMetadata] = {
    //return racks for brokers where replica count is over fair value
    brokerReplicaCounts
      .filter(_._2 < brokerFairReplicaValue)
      .keys.toSeq.distinct
  }

  //Summarise the topology as BrokerMetadata -> ReplicaCount
  def brokerReplicaCounts() = mutable.LinkedHashMap(
    brokersToReplicas()
      .map { case (x, y) => (x, y.size) }
      .sortBy(_._2)
      : _*
  )

  private def rackReplicaCounts() = mutable.LinkedHashMap(
    brokersToReplicas()
      .map { case (x, y) => (x, y.size) }
      .groupBy(_._1.rack.get)
      .mapValues(_.map(_._2).sum)
      .toSeq
      .sortBy(_._2)
      : _*
  )

  //Define rackFairValue: floor(replica-count / rack-count) replicas
  def rackFairReplicaValue() = Math.floor(
    brokerReplicaCounts.values.sum /
      rackCount
  )

  //Define  floor(replica-count / broker-count) replicas
  private def brokerFairReplicaValue() = Math.floor(
    brokerReplicaCounts.values.sum /
      brokerReplicaCounts
        .keys.toSeq.distinct.size
  )

  private def countFromPar(rack: String): Int = {
    Math.abs(rackReplicaCounts.get(rack).get - rackFairReplicaValue.toInt)
  }

  private def countFromPar(broker: BrokerMetadata): Int = {
    Math.abs(brokerReplicaCounts.get(broker).get - brokerFairReplicaValue.toInt)
  }

}


