package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc.fairness.{LeaderFairness, ReplicaFairness}

import scala.collection.{Iterable, Seq, Map}
import scala.util.Random

class ByBroker(allBrokers: Seq[BrokerMetadata], p: Map[TopicAndPartition, Seq[Int]], rack: String) extends ClusterView  with TopologyFactory with TopologyHelper {

  val constraints = new Constraints(allBrokers, p)
  val brokers = allBrokers.filter(_.rack.get == rack)
  val partitions = filter(rack, allBrokers, p)
  val brokersToReplicas = createBrokersToReplicas(allBrokers, allBrokers, p).filter(_._1.rack.get == rack)
  val brokersToLeaders = createBrokersToLeaders(allBrokers, allBrokers, p).filter(_._1.rack.get == rack)
  val brokersToNonLeaders = createBrokersToNonLeaders(allBrokers, brokers, p).filter(_._1.rack.get == rack)
  val replicaFairness = new ReplicaFairness(brokersToReplicas, brokers)
  val leaderFairness = new LeaderFairness(brokersToLeaders, brokers)

  def replicasOnAboveParBrokers(): Seq[Replica] = replicaFairness.aboveParBrokers.flatMap(weightedReplicasFor(_, brokersToReplicas))

  def brokersWithBelowParReplicaCount(): Seq[BrokerMetadata] = replicaFairness.belowParBrokers

  def leadersOnAboveParBrokers(): Seq[TopicAndPartition] = leaderFairness.aboveParBrokers.flatMap(leadersOn(_, brokersToLeaders))

  def brokersWithBelowParLeaderCount(): Seq[BrokerMetadata] = leaderFairness.belowParBrokers

  def refresh(newPartitionsMap: Map[TopicAndPartition, Seq[Int]]): ClusterView = new ByBroker(allBrokers, newPartitionsMap, rack)

  //TODO - really need to make brokersToReplicas a map not a list!
  def nonLeadReplicasFor(broker: BrokerMetadata): Seq[Replica] = brokersToNonLeaders.filter(_._1.id == broker.id).last._2

}




