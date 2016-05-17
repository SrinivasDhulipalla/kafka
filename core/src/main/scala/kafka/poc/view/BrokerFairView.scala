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

  val partitionsOnRack = filter(rack, allBrokers, allPartitions)
  val brokersOnRack = allBrokers.filter(rack == null || _.rack.get == rack)
  val constraints = new Constraints(brokersOnRack, partitionsOnRack)

  val brokersToReplicas = rackFilter(createBrokersToReplicas(allBrokers, allPartitions))
  val brokersToLeaders = rackFilter(createBrokersToLeaders(allBrokers, allPartitions))
  val brokersToNonLeaders = rackFilter(createBrokersToNonLeaders(allBrokers, allPartitions))

  val replicaFairness = new ReplicaFairness(brokersToReplicas, brokersOnRack)
  val leaderFairness = new LeaderFairness(brokersToLeaders, brokersOnRack)

  override def replicasOnAboveParBrokers(): Seq[Replica] = replicaFairness.aboveParBrokers.flatMap(weightedReplicasFor(_, brokersToReplicas))

  override def brokersWithBelowParReplicaCount(): Seq[BrokerMetadata] = replicaFairness.belowParBrokers

  override def leadersOnAboveParBrokers(): Seq[TopicAndPartition] = leaderFairness.aboveParBrokers.flatMap(leadersOn(_, brokersToLeaders))

  override def brokersWithBelowParLeaderCount(): Seq[BrokerMetadata] = leaderFairness.belowParBrokers

  override def refresh(newPartitionsMap: Map[TopicAndPartition, Seq[Int]]): ClusterView = new BrokerFairView(allBrokers, newPartitionsMap, rack)

  override def nonFollowersOn(broker: BrokerMetadata): Seq[Replica] = brokersToNonLeaders.filter(_._1.id == broker.id).last._2

  def hasReplicaFairnessImprovement(b1: Int, b2: Int): Boolean ={
    val repliacsOnB1: Int = brokersToReplicas.filter(_._1.id ==b1).last._2.size
    val repliacsOnB2: Int = brokersToReplicas.filter(_._1.id ==b2).last._2.size
    repliacsOnB1 > repliacsOnB2 + 1
  }

  def hasLeaderFairnessImprovement(b1: Int, b2: Int): Boolean ={
    val leadersOnB1: Int = brokersToLeaders.filter(_._1.id ==b1).last._2.size
    val leadersOnB2: Int = brokersToLeaders.filter(_._1.id ==b2).last._2.size
    leadersOnB1 > leadersOnB2 + 1
  }


}