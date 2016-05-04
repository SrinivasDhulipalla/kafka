package kafka.poc

import kafka.common.TopicAndPartition

import scala.collection.Map

trait RebalanceConstraints {

  def obeysRackConstraint(partition: TopicAndPartition, brokerFrom: Int, brokerTo: Int, replicationFactors: Map[String, Int]): Boolean

  def obeysPartitionConstraint(replica: TopicAndPartition, brokerMovingTo: Int): Boolean
}
