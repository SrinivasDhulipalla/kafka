package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc.fairness.{LeaderFairness, ReplicaFairness}

import scala.collection.{Iterable, Seq, Map}

class ByRack(allBrokers: Seq[BrokerMetadata], allPartitions: Map[TopicAndPartition, Seq[Int]]) extends BaseSomething with ClusterView with TopologyFactory with TopologyHelper {

  val constraints = new Constraints(allBrokers, allPartitions)

  val brokersToReplicas = createBrokersToReplicas(allBrokers, allBrokers, allPartitions)
  val brokersToLeaders = createBrokersToLeaders(allBrokers, allBrokers, allPartitions)

  val replicaFairness = new ReplicaFairness(brokersToReplicas, allBrokers)
  val leaderFairness = new LeaderFairness(brokersToLeaders, allBrokers)

  def replicasOnAboveParBrokers(): Seq[Replica] = replicaFairness.aboveParRacks.flatMap(weightedReplicasFor(_, brokersToReplicas))

  def brokersWithBelowParReplicaCount(): Seq[BrokerMetadata] = replicaFairness.belowParRacks.flatMap(leastLoadedBrokerIds(_, brokersToReplicas))

  def leadersOnAboveParBrokers(): Seq[TopicAndPartition] = leaderFairness.aboveParRacks.flatMap(leadersOn(_, brokersToLeaders))

  def brokersWithBelowParLeaderCount(): Seq[BrokerMetadata] = brokersOn(leaderFairness.belowParRacks, allBrokers)

  def refresh(newPartitionsMap: Map[TopicAndPartition, Seq[Int]]): ClusterView = new ByRack(allBrokers, newPartitionsMap)

  def nonLeadReplicasFor(brokerMetadata: BrokerMetadata): scala.Seq[Replica] =  Seq()
}

