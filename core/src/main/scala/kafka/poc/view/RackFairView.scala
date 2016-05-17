package kafka.poc.view

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc.constraints.Constraints
import kafka.poc.fairness.{LeaderFairness, ReplicaFairness}
import kafka.poc.topology.{Replica, TopologyHelper, TopologyFactory}

import scala.collection.{Map, Seq}

class RackFairView(allBrokers: Seq[BrokerMetadata], allPartitions: Map[TopicAndPartition, Seq[Int]]) extends ClusterView with TopologyFactory with TopologyHelper {
  val constraints = new Constraints(allBrokers, allPartitions)

  val brokersToReplicas = createBrokersToReplicas(allBrokers, allPartitions)
  val brokersToLeaders = createBrokersToLeaders(allBrokers, allPartitions)

  val replicaFairness = new ReplicaFairness(brokersToReplicas, allBrokers)
  val leaderFairness = new LeaderFairness(brokersToLeaders, allBrokers)
  val brokersToNonLeaders = createBrokersToNonLeaders(allBrokers, allPartitions)

  override def replicasOnAboveParBrokers(): Seq[Replica] = replicaFairness.aboveParRacks.flatMap(weightedReplicasFor(_, brokersToReplicas))

  override def brokersWithBelowParReplicaCount(): Seq[BrokerMetadata] = replicaFairness.belowParRacks.flatMap(leastLoadedBrokerIds(_, brokersToReplicas))

  override def leadersOnAboveParBrokers(): Seq[TopicAndPartition] = leaderFairness.aboveParRacks.flatMap(leadersOn(_, brokersToLeaders))

  override def brokersWithBelowParLeaderCount(): Seq[BrokerMetadata] = brokersOn(leaderFairness.belowParRacks, allBrokers)

  override def refresh(newPartitionsMap: Map[TopicAndPartition, Seq[Int]]): ClusterView = new RackFairView(allBrokers, newPartitionsMap)

  override def nonFollowersOn(brokerMetadata: BrokerMetadata): scala.Seq[Replica] = Seq()

  override def hasReplicaFairnessImprovement(b1: Int, b2: Int): Boolean =
    filterReplicas(rack(b1)) > filterReplicas(rack(b2)) + 1

  override def hasLeaderFairnessImprovement(b1: Int, b2: Int): Boolean =
    filterLeaders(rack(b1)) > filterLeaders(rack(b2)) + 1

  private def rack(b1: Int): String = bk(b1).rack.get

  private def filterLeaders(rack1: String): Int =
    brokersToLeaders.filter(_._1.rack.get == rack1).map(_._2).flatten.size

  private def filterReplicas(rack1: String): Int =
    brokersToReplicas.filter(_._1.rack.get == rack1).map(_._2).flatten.size

  private def bk(id: Int): BrokerMetadata = allBrokers.filter(_.id == id).last
}

