package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection._


class MovesOptimisedRebalancePolicy extends RabalancePolicy {

  override def rebalancePartitions(brokers: Seq[BrokerMetadata], replicasForPartitions: Map[TopicAndPartition, Seq[Int]], replicationFactors: Map[String, Int]): Map[TopicAndPartition, Seq[Int]] = {
    val partitions = collection.mutable.Map(replicasForPartitions.toSeq: _*) //todo deep copy
    val replicaFilter = new ReplicaFilter(brokers, partitions)

    //Ensure partitions are fully replicated
    for (partition <- partitions.keys) {
      def replicationFactor = replicationFactors.get(partition.topic).get
      def replicasForP = partitions.get(partition).get

      while (replicasForP.size < replicationFactor) {
        val leastLoadedBrokers = replicaFilter.leastLoadedBrokersDownranking(replicaFilter.racksFor(partition))
        val leastLoadedButNoExistingReplica = leastLoadedBrokers.filterNot(replicasForP.toSet).last
        partitions.put(partition, replicasForP :+ leastLoadedButNoExistingReplica)
      }
    }

    // Optimise for replica fairness across racks
    for(aboveParRack <- replicaFilter.aboveParRacks()){
      for(replicaToMove <- replicaFilter.weightedReplicasFor(aboveParRack)){
          for(belowParRack <-  replicaFilter.belowParRacks){
            for (broker <- replicaFilter.leastLoadedBrokers(belowParRack)) {
              val partition = replicaToMove.topicAndPartition
              var replicas = partitions.get(partition).get
              //remove the old broker from the replicas list
              replicas = replicas.filter(_ == replicaToMove.broker)
              //add the new broker to the replicas list
              replicas = replicas :+ broker
              partitions.put(partition, replicas)
            }
          }
      }
    }

    partitions
  }
}
