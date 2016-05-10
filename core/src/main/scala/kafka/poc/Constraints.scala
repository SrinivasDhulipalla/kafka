package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc.fairness.{LeaderFairness, ReplicaFairness, Fairness}

import scala.collection._
import collection.mutable.LinkedHashMap


class Constraints(allBrokers: Seq[BrokerMetadata], partitions: Map[TopicAndPartition, Seq[Int]]) extends BaseSomething with TopologyHelper with TopologyFactory {

  var brokersToReplicas = createBrokersToReplicas(allBrokers, allBrokers, partitions)
  private def bk(id: Int): BrokerMetadata = allBrokers.filter(_.id == id).last


  object constraints extends RebalanceConstraints {
    def obeysRackConstraint(partition: TopicAndPartition, brokerFrom: Int, brokerTo: Int, replicationFactors: Map[String, Int]): Boolean = {
      val minRacksSpanned = Math.min(replicationFactors.get(partition.topic).get, rackCount(allBrokers))

      //get replicas for partition, replacing brokerFrom with brokerTo
      var proposedReplicas: Seq[Int] = partitions.get(partition).get
      val index: Int = proposedReplicas.indexOf(brokerFrom)
      proposedReplicas = proposedReplicas.patch(index, Seq(brokerTo), 1)

      //find how many racks are now spanned
      val racksSpanned = proposedReplicas.map(bk(_)).map(_.rack).distinct.size

      racksSpanned >= minRacksSpanned
    }

    def obeysPartitionConstraint(replica: TopicAndPartition, brokerMovingTo: Int): Boolean = {
      !replicasFor(brokersToReplicas, brokerMovingTo).map(_.partition).contains(replica)
    }
  }



}

