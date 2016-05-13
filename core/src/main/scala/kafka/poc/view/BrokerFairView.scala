package kafka.poc.view

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc._
import kafka.poc.constraints.Constraints
import kafka.poc.fairness.{LeaderFairness, ReplicaFairness}
import kafka.poc.topology.{Replica, TopologyHelper, TopologyFactory}

import scala.collection.{Map, Seq}

class BrokerFairView(allBrokers: Seq[BrokerMetadata], allPartitions: Map[TopicAndPartition, Seq[Int]], rack: String) extends ClusterView with TopologyFactory with TopologyHelper {
  def rackFilter[T](map: Seq[(BrokerMetadata, T)]) = map.filter(rack == null || _._1.rack.get == rack)

  val constraints = new Constraints(allBrokers, allPartitions)
  val brokersOnRack = allBrokers.filter(rack == null || _.rack.get == rack)

  val brokersToReplicas = rackFilter(createBrokersToReplicas(allBrokers, allBrokers, allPartitions))
  val brokersToLeaders = rackFilter(createBrokersToLeaders(allBrokers, allBrokers, allPartitions))
  val brokersToNonLeaders = rackFilter(createBrokersToNonLeaders(allBrokers, brokersOnRack, allPartitions))

  val replicaFairness = new ReplicaFairness(brokersToReplicas, brokersOnRack)
  val leaderFairness = new LeaderFairness(brokersToLeaders, brokersOnRack)

  override def replicasOnAboveParBrokers(): Seq[Replica] = replicaFairness.aboveParBrokers.flatMap(weightedReplicasFor(_, brokersToReplicas))

  override def brokersWithBelowParReplicaCount(): Seq[BrokerMetadata] = replicaFairness.belowParBrokers

  override def leadersOnAboveParBrokers(): Seq[TopicAndPartition] = leaderFairness.aboveParBrokers.flatMap(leadersOn(_, brokersToLeaders))

  override def brokersWithBelowParLeaderCount(): Seq[BrokerMetadata] = leaderFairness.belowParBrokers

  override def refresh(newPartitionsMap: Map[TopicAndPartition, Seq[Int]]): ClusterView = new BrokerFairView(allBrokers, newPartitionsMap, rack)

  override def nonLeadReplicasFor(broker: BrokerMetadata): Seq[Replica] = brokersToNonLeaders.filter(_._1.id == broker.id).last._2
}