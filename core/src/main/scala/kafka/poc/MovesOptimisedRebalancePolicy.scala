package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection._


class MovesOptimisedRebalancePolicy extends RabalancePolicy {

  override def rebalancePartitions(brokers: Seq[BrokerMetadata], replicasForPartitions: Map[TopicAndPartition, Seq[Int]], replicationFactors: Map[String, Int]): Map[TopicAndPartition, Seq[Int]] = {
    val partitionsMap = collection.mutable.Map(replicasForPartitions.toSeq: _*) //todo deep copy
    val cluster = new ReplicaFilter(brokers, partitionsMap)

    //Step 1: Ensure partitions are fully replicated
    for (partition <- partitionsMap.keys) {
      def replicationFactor = replicationFactors.get(partition.topic).get
      def replicasForP = partitionsMap.get(partition).get

      while (replicasForP.size < replicationFactor) {
        val leastLoadedBrokers = cluster.leastLoadedBrokersDownranking(cluster.racksFor(partition))
        val leastLoadedButNoExistingReplica = leastLoadedBrokers.filterNot(replicasForP.toSet).last
        partitionsMap.put(partition, replicasForP :+ leastLoadedButNoExistingReplica)
        println(s"Additional replica was created on broker [$leastLoadedButNoExistingReplica] for under-replicated partition [$partition].")
      }
    }

    // Step 2.1: Optimise for replica fairness across racks
    for (aboveParRack <- cluster.replicaFairness.aboveParRacks()) {
      for (replicaToMove <- cluster.weightedReplicasFor(aboveParRack)) {
        for (belowParRack <- cluster.replicaFairness.belowParRacks) {
          for (belowParBroker <- cluster.leastLoadedBrokerIds(belowParRack)) {
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
      //for each leader (could be optimised to be for(n) where n is the number we expect to move)
      for (leader <- cluster.leadersOn(aboveParRack)) {
        //ensure rack is still above par (could be optimised out as above)
        if (cluster.leaderFairness.aboveParRacks().contains(aboveParRack)) {
          //check to see if the partition has a non-leader replica on below par racks
          for (replica <- partitionsMap.get(leader).get.drop(1)) {
            if (cluster.brokersOn(cluster.leaderFairness.belowParRacks()).contains(replica)) {
              //if so, switch leadership
              makeLeader(leader, replica, partitionsMap)
            }
          }
        }
      }
    }

    //Step 3.1: Optimise for replica fairness across brokers
    for (aboveParBroker <- cluster.replicaFairness.aboveParBrokers) {
      for (replicaToMove <- cluster.weightedReplicasFor(aboveParBroker)) {
        for (belowParBroker <- cluster.replicaFairness.belowParBrokers) {
          val partition = replicaToMove.topicAndPartition
          val brokerFrom: Int = replicaToMove.broker
          val brokerTo: Int = belowParBroker.id
          move(partition, brokerFrom, brokerTo, partitionsMap)
        }
      }
    }

    //Step 3.2: Optimise for leader fairness across brokers
    //consider leaders on above par brokers
    for (aboveParBroker <- cluster.leaderFairness.aboveParBrokers()) {
      //for each leader (could be optimised to be for(n) where n is the number we expect to move)
      for (leader <- cluster.leadersOn(aboveParBroker)) {
        //ensure broker is still above par (could be optimised out as above)
        if (cluster.leaderFairness.aboveParBrokers().contains(aboveParBroker)) {
          //check to see if the partition has a non-leader replica on below par brokers
          for (replica <- partitionsMap.get(leader).get.drop(1)) {
            if (cluster.leaderFairness.belowParBrokers().map(_.id).contains(replica)) {
              //if so, switch leadership
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
    var currentLead = replicas(0)
    //remove old
    replicas = replicas.filter(_ != toPromote)
    //put first
    replicas = Seq(toPromote) ++ replicas
    partitionsMap.put(tp, replicas)
    println(s"Leadership moved brokers: [$currentLead -> $toPromote] for partition $tp")
  }

  def move(tp: TopicAndPartition, from: Int, to: Int, partitionsMap: collection.mutable.Map[TopicAndPartition, Seq[Int]]): Unit = {
    var replicas = partitionsMap.get(tp).get
    //remove old
    replicas = replicas.filter(_ != from)
    //add new
    replicas = replicas :+ to
    partitionsMap.put(tp, replicas)
    println(s"Partition $tp was moved from broker [$from] to [$to]")
  }
}
