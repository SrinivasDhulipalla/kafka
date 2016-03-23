package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection.Seq
import scala.collection.Map

trait RabalancePolicy {
  def rebalancePartitions(brokers: Seq[BrokerMetadata], currentAssignment: Map[TopicAndPartition, Seq[Int]], replicationFactors: Map[String, Int]): Map[TopicAndPartition, Seq[Int]]
}
