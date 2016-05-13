package kafka.poc.constraints

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc.topology.TopologyHelper
import kafka.poc.topology.{TopologyHelper, TopologyFactory}

import scala.collection._


class Constraints(allBrokers: Seq[BrokerMetadata], partitions: Map[TopicAndPartition, Seq[Int]]) extends TopologyHelper with TopologyFactory with RebalanceConstraints {

  private val brokersToReplicas = createBrokersToReplicas(allBrokers, allBrokers, partitions)

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

    racksSpanned >= minRacksSpanned
  }

  override def obeysPartitionConstraint(partition: TopicAndPartition, brokerMovingTo: Int): Boolean = {

    val replicas = partitions.get(partition).get
    val result: Boolean = !replicas.contains(brokerMovingTo)
    if(!result)
      println(s"Failing partition constraint for $partition:${partitions.get(partition)} -> $brokerMovingTo ")
    result
  }
}