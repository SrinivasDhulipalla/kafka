package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection.{Seq, Map}

trait ClusterView {
  def printBrokerToLeaderMap()


  def aboveParReplicas(): scala.Seq[Replica]

  def belowParBrokers(): scala.Seq[BrokerMetadata]

  def aboveParLeaders(): scala.Seq[TopicAndPartition]

  def brokersWithBelowParLeaders(): scala.Seq[BrokerMetadata]

  def constraints(): Constraints

  def refresh(partitionMap: Map[TopicAndPartition, Seq[Int]]): ClusterView

  def nonLeadReplicasFor(brokerMetadata: BrokerMetadata): scala.Seq[Replica]
}

