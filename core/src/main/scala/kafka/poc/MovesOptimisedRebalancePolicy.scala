package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection._


class MovesOptimisedRebalancePolicy extends RabalancePolicy {
  override def rebalancePartitions(brokers: Seq[BrokerMetadata], currentAssignment: Map[TopicAndPartition, Seq[Int]], replicationFactors: Map[String, Int]): Map[TopicAndPartition, Seq[Int]] = {
    val assignment = collection.mutable.Map(currentAssignment.toSeq: _*) //todo deep copy

    //Sum replica counts for each broker
    val brokerToReplicaCounts: mutable.LinkedHashMap[Int, Int] = mutable.LinkedHashMap(
      assignment.values
        .flatMap(x => x)
        .groupBy(x => x)
        .map { case (x, y) => (x, y.size) }
        .toSeq
        .sortBy(_._2)
        : _*
    )
    var mostLoadedBrokers = brokerToReplicaCounts.keys
    val emptyBrokers = brokers.map(_.id).filterNot(mostLoadedBrokers.toSet)
    mostLoadedBrokers =  emptyBrokers.toSeq ++ mostLoadedBrokers.toSeq

    println(currentAssignment)
    println("brokerToReplicaCounts "+brokerToReplicaCounts)
    println("most loaded "+brokerToReplicaCounts)
    println("most loaded with empty brokers "+mostLoadedBrokers)

    //Ensure partitions are fully replicated
    for (partition <- assignment.keys) {
      def replicationFactor = {replicationFactors.get(partition.topic).get}
      def existingReplicas = {assignment.get(partition).get}

      while (existingReplicas.size < replicationFactor) {
        val leastLoaded = mostLoadedBrokers.filterNot(existingReplicas.toSet).toSeq(0)
        assignment.put(partition, existingReplicas :+ leastLoaded)
      }
    }

    assignment
  }
}
