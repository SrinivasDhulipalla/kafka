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

  def bk(id: Int): BrokerMetadata = allBrokers.filter(_.id == id).last

  def hasReplicaFairnessImprovement(b1: Int, b2: Int): Boolean = {
    val rack1 = bk(b1).rack.get
    val rack2 = bk(b2).rack.get
    val repliacsOnR1: Int = brokersToReplicas.filter(_._1.rack.get == rack1).map(_._2).flatten.size
    val repliacsOnR2: Int = brokersToReplicas.filter(_._1.rack.get == rack2).map(_._2).flatten.size

    val result: Boolean = repliacsOnR1 > repliacsOnR2 + 1

    if(!result) {
      println(s"Failed replica fairness improvement. Reps on rack1 $repliacsOnR1, Reps on rack2 $repliacsOnR2")
      println(brokersToReplicas.filter(_._1.rack.get == rack1))
      println(brokersToReplicas.filter(_._1.rack.get == rack2))
    }

    result
  }

  def hasLeaderFairnessImprovement(b1: Int, b2: Int): Boolean = {
    val rack1 = bk(b1).rack.get
    val rack2 = bk(b2).rack.get
    val leadersOnR1: Int = brokersToLeaders.filter(_._1.rack.get == rack1).map(_._2).flatten.size
    val leadersOnR2: Int = brokersToLeaders.filter(_._1.rack.get == rack1).map(_._2).flatten.size
    leadersOnR1 > leadersOnR2 + 1
  }
}

