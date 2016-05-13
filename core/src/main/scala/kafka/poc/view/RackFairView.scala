package kafka.poc.view

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc.constraints.Constraints
import kafka.poc.fairness.{LeaderFairness, ReplicaFairness}
import kafka.poc.topology.{Replica, TopologyHelper, TopologyFactory}

import scala.collection.{Map, Seq}

class RackFairView(allBrokers: Seq[BrokerMetadata], allPartitions: Map[TopicAndPartition, Seq[Int]]) extends ClusterView with TopologyFactory with TopologyHelper {
  val constraints = new Constraints(allBrokers, allPartitions)

  val brokersToReplicas = createBrokersToReplicas(allBrokers, allBrokers, allPartitions)
  val brokersToLeaders = createBrokersToLeaders(allBrokers, allBrokers, allPartitions)

  val replicaFairness = new ReplicaFairness(brokersToReplicas, allBrokers)
  val leaderFairness = new LeaderFairness(brokersToLeaders, allBrokers)

  override def replicasOnAboveParBrokers(): Seq[Replica] = replicaFairness.aboveParRacks.flatMap(weightedReplicasFor(_, brokersToReplicas))

  override def brokersWithBelowParReplicaCount(): Seq[BrokerMetadata] = replicaFairness.belowParRacks.flatMap(leastLoadedBrokerIds(_, brokersToReplicas))

  override def leadersOnAboveParBrokers(): Seq[TopicAndPartition] = leaderFairness.aboveParRacks.flatMap(leadersOn(_, brokersToLeaders))

  override def brokersWithBelowParLeaderCount(): Seq[BrokerMetadata] = brokersOn(leaderFairness.belowParRacks, allBrokers)

  override def refresh(newPartitionsMap: Map[TopicAndPartition, Seq[Int]]): ClusterView = new RackFairView(allBrokers, newPartitionsMap)

  override def nonLeadReplicasFor(brokerMetadata: BrokerMetadata): scala.Seq[Replica] =  Seq()
}

