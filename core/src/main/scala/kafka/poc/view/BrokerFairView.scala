package kafka.poc.view

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc.constraints.Constraints
import kafka.poc.fairness.{LeaderFairness, ReplicaFairness}
import kafka.poc.topology.{Replica, TopologyFactory, TopologyHelper}

import scala.collection.{Map, Seq}

class BrokerFairView(allBrokers: Seq[BrokerMetadata], allPartitions: Map[TopicAndPartition, Seq[Int]], rack: String) extends ClusterView with TopologyFactory with TopologyHelper {

  val partitionsOnRack = filter(rack, allBrokers, allPartitions)
  val brokersOnRack = allBrokers.filter(rack == null || _.rack.get == rack)
  val constraints = new Constraints(brokersOnRack, partitionsOnRack)

  val brokersToReplicas = rackFilter(createBrokersToReplicas(allBrokers, allPartitions))
  val brokersToLeaders = rackFilter(createBrokersToLeaders(allBrokers, allPartitions))
  val brokersToNonLeaders = rackFilter(createBrokersToFollowers(allBrokers, allPartitions))

  val replicaFairness = new ReplicaFairness(brokersToReplicas, brokersOnRack)
  val leaderFairness = new LeaderFairness(brokersToLeaders, brokersOnRack)

  override def replicasOnAboveParBrokers(): Seq[Replica] = replicaFairness.aboveParBrokers.flatMap(weightedReplicasFor(_, brokersToReplicas))

  override def brokersWithBelowParReplicaCount(): Seq[BrokerMetadata] = replicaFairness.belowParBrokers

  override def leadersOnAboveParBrokers(): Seq[TopicAndPartition] = leaderFairness.aboveParBrokers.flatMap(leadersOn(_, brokersToLeaders))

  override def brokersWithBelowParLeaderCount(): Seq[BrokerMetadata] = leaderFairness.belowParBrokers

  override def refresh(newPartitionsMap: Map[TopicAndPartition, Seq[Int]]): ClusterView = new BrokerFairView(allBrokers, newPartitionsMap, rack)

  override def nonFollowersOn(broker: BrokerMetadata): Seq[Replica] = brokersToNonLeaders.filter(_._1.id == broker.id).last._2

  override def improvesReplicaFairness(b1: Int, b2: Int): Boolean = filterReplicas(b1) > filterReplicas(b2) + 1

  override def improvesLeaderFairness(b1: Int, b2: Int): Boolean = filterLeaders(b1) > filterLeaders(b2) + 1


  private def filterReplicas(b1: Int): Int = brokersToReplicas.filter(_._1.id == b1).last._2.size

  private def filterLeaders(b1: Int): Int = brokersToLeaders.filter(_._1.id == b1).last._2.size

  private def rackFilter[T](map: Seq[(BrokerMetadata, T)]) = map.filter(rack == null || _._1.rack.get == rack)
}