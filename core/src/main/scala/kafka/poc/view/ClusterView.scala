package kafka.poc.view

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc.constraints.Constraints
import kafka.poc.topology.Replica

import scala.collection.{Map, Seq}

trait ClusterView {

  def replicasOnAboveParBrokers(): scala.Seq[Replica]

  def brokersWithBelowParReplicaCount(): scala.Seq[BrokerMetadata]

  def leadersOnAboveParBrokers(): scala.Seq[TopicAndPartition]

  def brokersWithBelowParLeaderCount(): scala.Seq[BrokerMetadata]

  def constraints(): Constraints

  def refresh(partitionMap: Map[TopicAndPartition, Seq[Int]]): ClusterView

  def nonFollowersOn(brokerMetadata: BrokerMetadata): scala.Seq[Replica]

  def improvesReplicaFairness(b1: Int, b2: Int): Boolean

  def improvesLeaderFairness(b1: Int, b2: Int): Boolean
}

