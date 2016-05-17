package kafka.poc.fairness

import kafka.admin.BrokerMetadata
import kafka.poc.topology.{Replica, TopologyHelper}

import scala.collection.{Seq}

/**
  * Class describes the concept of replica fairness by returning above/below par racks and brokers.
  *
  * @see kafka.poc.fairness.Fairness
  *
  * @param brokersToReplicas
  * @param allBrokers
  */
class ReplicaFairness(brokersToReplicas: Seq[(BrokerMetadata, Seq[Replica])], allBrokers: Seq[BrokerMetadata]) extends Fairness with TopologyHelper {

  private val rackReplicaCounts = getRackReplicaCounts(brokersToReplicas)
  private val brokerReplicaCounts = getBrokerReplicaCounts(brokersToReplicas)
  private val rackCount: Int = allBrokers.map(_.rack.get).distinct.size
  private val replicaCount: Float = brokerReplicaCounts.values.sum.toFloat
  val rackFairValue: Float = replicaCount / rackCount
  val brokerFairValue: Float = replicaCount / allBrokers.size

  override def aboveParRacks(): Seq[String] =
    rackReplicaCounts.filter { x => x._2.toInt > Math.floor(rackFairValue).toInt }.keys.toSeq.distinct

  override def belowParRacks(): Seq[String] =
    rackReplicaCounts.filter(_._2.toInt < Math.ceil(rackFairValue).toInt).keys.toSeq.distinct

  override def aboveParBrokers(): Seq[BrokerMetadata] =
    brokerReplicaCounts.filter(_._2.toInt > Math.floor(brokerFairValue).toInt).keys.toSeq.distinct

  override def belowParBrokers(): Seq[BrokerMetadata] =
    brokerReplicaCounts.filter(_._2 < Math.ceil(brokerFairValue).toInt).keys.toSeq.distinct


}