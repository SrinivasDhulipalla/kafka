package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection._


class MovesOptimisedRebalancePolicy extends RabalancePolicy {

  override def rebalancePartitions(brokers: Seq[BrokerMetadata], replicasForPartitions: Map[TopicAndPartition, Seq[Int]], replicationFactors: Map[String, Int]): Map[TopicAndPartition, Seq[Int]] = {
    val partitionsMap = collection.mutable.Map(replicasForPartitions.toSeq: _*) //todo deep copy
    val cluster = new ReplicaFilter(brokers, partitionsMap)

    //Ensure partitions are fully replicated
    for (partition <- partitionsMap.keys) {
      def replicationFactor = replicationFactors.get(partition.topic).get
      def replicasForP = partitionsMap.get(partition).get

      while (replicasForP.size < replicationFactor) {
        val leastLoadedBrokers = cluster.leastLoadedBrokersDownranking(cluster.racksFor(partition))
        val leastLoadedButNoExistingReplica = leastLoadedBrokers.filterNot(replicasForP.toSet).last
        partitionsMap.put(partition, replicasForP :+ leastLoadedButNoExistingReplica)
      }
    }

    // Optimise for replica fairness across racks
    for (aboveParRack <- cluster.aboveParRacks()) {
      for (replicaToMove <- cluster.weightedReplicasFor(aboveParRack)) {
        for (belowParRack <- cluster.belowParRacks) {
          for (belowParBroker <- cluster.leastLoadedBrokers(belowParRack)) {
            val partition = replicaToMove.topicAndPartition
            val brokerFrom: Int = replicaToMove.broker
            val brokerTo: Int = belowParBroker
            move(partition, brokerFrom, brokerTo, partitionsMap)
          }
        }
      }
    }

    //Step 2.2: Optimise for leader fairness across racks

    //consider leaders on above par racks
    for (aboveParRack <- cluster.leaderFairness.aboveParRacks()) {
      for (leader <- cluster.leadersOn(aboveParRack)) {
        if (cluster.leaderFairness.aboveParRacks().contains(aboveParRack)) {
          //check to see if the partition has repilca on low par racks
          for (replica <- partitionsMap.get(leader).get.drop(1)) {
            //if so, switch leadership
            if (cluster.brokersOn(cluster.leaderFairness.belowParRacks()).contains(replica)) {
              makeLeader(leader, replica, partitionsMap)
            }
          }
        }
      }
    }
    partitionsMap
  }

  def makeLeader(tp: TopicAndPartition, toPromote: Int, partitionsMap: collection.mutable.Map[TopicAndPartition, Seq[Int]]): Unit = {
    var replicas = partitionsMap.get(tp).get
    //remove old
    replicas = replicas.filter(_ != toPromote)
    //put first
    replicas = Seq(toPromote) ++ replicas
    partitionsMap.put(tp, replicas)
  }

  def move(tp: TopicAndPartition, from: Int, to: Int, partitionsMap: collection.mutable.Map[TopicAndPartition, Seq[Int]]): Unit = {
    var replicas = partitionsMap.get(tp).get
    //remove old
    replicas = replicas.filter(_ != from)
    //add new
    replicas = replicas :+ to
    partitionsMap.put(tp, replicas)
  }
}
