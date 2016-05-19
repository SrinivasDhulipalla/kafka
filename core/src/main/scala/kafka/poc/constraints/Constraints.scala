package kafka.poc.constraints

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc.topology.TopologyHelper
import kafka.poc.topology.{TopologyHelper, TopologyFactory}
import kafka.utils.Logging

import scala.collection._


class Constraints(allBrokers: Seq[BrokerMetadata], partitions: Map[TopicAndPartition, Seq[Int]]) extends TopologyHelper with TopologyFactory with RebalanceConstraints with Logging {
  private def bk(id: Int): BrokerMetadata = allBrokers.filter(_.id == id).last

  override def obeysRackConstraint(partition: TopicAndPartition, brokerFrom: Int, brokerTo: Int, replicationFactors: Map[String, Int]): Boolean = {
    val minRacksSpanned = Math.min(replicationFactors.get(partition.topic).get, rackCount(allBrokers))

    //get replicas for partition, replacing brokerFrom with brokerTo
    var proposedReplicas = partitions.get(partition).get

    val index: Int = proposedReplicas.indexOf(brokerFrom)
    proposedReplicas = index match {
      case -1 => proposedReplicas ++ Seq(brokerTo)
      case _ => proposedReplicas.patch(index, Seq(brokerTo), 1)
    }

    //find how many racks are now spanned
    val racksSpanned = proposedReplicas.map(bk(_)).map(_.rack).distinct.size

    val result = racksSpanned >= minRacksSpanned

    if (!result)
      debug(s"failing rack constraint $partition:$proposedReplicas, $brokerFrom -> $brokerTo ")

    result
  }

  override def obeysPartitionConstraint(partition: TopicAndPartition, brokerMovingTo: Int): Boolean = {

    val replicas = partitions.get(partition).get
    val result = !replicas.contains(brokerMovingTo)
    if (!result)
      debug(s"Failing partition constraint for $partition:${partitions.get(partition)} -> $brokerMovingTo ")

    result
  }
}