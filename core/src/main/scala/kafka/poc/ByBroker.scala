package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc.fairness.{LeaderFairness, ReplicaFairness}

import scala.collection.{Iterable, Seq, Map}

class ByBroker(allBrokers: Seq[BrokerMetadata], p: Map[TopicAndPartition, Seq[Int]], rack: String) extends ClusterView  with TopologyFactory with TopologyHelper {

  val constraints = new Constraints(allBrokers, p)
  val brokers = allBrokers.filter(_.rack.get == rack)
  val partitions = filter(rack, allBrokers, p)
  val brokersToReplicas = createBrokersToReplicas(allBrokers, brokers, p).filter(_._1.rack.get == rack)
  val brokersToLeaders = createBrokersToLeaders(allBrokers, brokers, p).filter(_._1.rack.get == rack)
  val replicaFairness = new ReplicaFairness(brokersToReplicas)
  val leaderFairness = new LeaderFairness(brokersToLeaders)

  def aboveParReplicas(): Seq[Replica] = replicaFairness.aboveParBrokers.flatMap(weightedReplicasFor(_, brokersToReplicas))

  def belowParBrokers(): Seq[BrokerMetadata] = replicaFairness.belowParBrokers

  def aboveParPartitions(): Seq[TopicAndPartition] = leaderFairness.aboveParBrokers.flatMap(leadersOn(_, brokersToLeaders))

  def brokersWithBelowParLeaders(): Seq[Int] = leaderFairness.belowParBrokers.map(_.id)

  def refresh(newPartitionsMap: Map[TopicAndPartition, Seq[Int]]): ClusterView = new ByBroker(allBrokers, newPartitionsMap, rack)

}




