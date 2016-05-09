package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection.{Seq, Map}

trait ClusterView {
  def aboveParReplicas(): scala.Seq[Replica]

  def belowParBrokers(): scala.Seq[BrokerMetadata]

  def aboveParPartitions(): scala.Seq[TopicAndPartition]

  def brokersWithBelowParLeaders(): scala.Seq[Int]

  def refresh(p: Map[TopicAndPartition, Seq[Int]]): Unit
}

