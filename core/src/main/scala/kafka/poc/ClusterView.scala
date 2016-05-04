package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

trait ClusterView {
  def aboveParReplicas(): scala.Seq[Replica]

  def belowParBrokers(): scala.Seq[BrokerMetadata]

  def aboveParLeaders(): scala.Seq[TopicAndPartition]

  def brokersWithBelowParLeaders(): scala.Seq[Int]
}

