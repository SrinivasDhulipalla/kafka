package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection._


class MovesOptimisedRebalancePolicy extends RabalancePolicy {

  override def rebalancePartitions(brokers: Seq[BrokerMetadata], replicasForPartitions: Map[TopicAndPartition, Seq[Int]], replicationFactors: Map[String, Int]): Map[TopicAndPartition, Seq[Int]] = {
    val partitions = collection.mutable.Map(replicasForPartitions.toSeq: _*) //todo deep copy
    val replicaFilter = new ReplicaFilter(brokers, partitions)


    def racksFor(p: TopicAndPartition) = {
      brokers.filter(broker =>
        partitions.get(p).get
          .contains(broker.id)
      ).map(_.rack.get)
    }

    //Ensure partitions are fully replicated
    for (partition <- partitions.keys) {
      def replicationFactor = replicationFactors.get(partition.topic).get
      def replicasForP = partitions.get(partition).get

      while (replicasForP.size < replicationFactor) {
        val leastLoadedBrokers = replicaFilter.leastLoadedBrokersDownranking(racksFor(partition))
        val leastLoadedButNoExistingReplica = leastLoadedBrokers.filterNot(replicasForP.toSet).last
        partitions.put(partition, replicasForP :+ leastLoadedButNoExistingReplica)
      }
    }

    partitions
  }
}
