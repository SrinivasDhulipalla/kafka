package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection.{Seq, Map}

trait ClusterView {
  def printBrokerToLeaderMap()


  def replicasOnAboveParBrokers(): scala.Seq[Replica]

  def brokersWithBelowParReplicaCount(): scala.Seq[BrokerMetadata]

  def leadersOnAboveParBrokers(): scala.Seq[TopicAndPartition]

  def brokersWithBelowParLeaderCount(): scala.Seq[BrokerMetadata]

  def constraints(): Constraints

  def refresh(partitionMap: Map[TopicAndPartition, Seq[Int]]): ClusterView

  def nonLeadReplicasFor(brokerMetadata: BrokerMetadata): scala.Seq[Replica]
}

