package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection._


class MovesOptimisedRebalancePolicy extends RabalancePolicy {


  override def rebalancePartitions(brokers: Seq[BrokerMetadata], replicasForPartitions: Map[TopicAndPartition, Seq[Int]], replicationFactors: Map[String, Int]): Map[TopicAndPartition, Seq[Int]] = {
    val partitionsMap = collection.mutable.Map(replicasForPartitions.toSeq: _*) //todo deep copy?
    val cluster = new ReplicaFilter(brokers, partitionsMap)
    println("\nBrokers: " + brokers.map { b => "\n" + b })
    print(partitionsMap, cluster)

    ensureFullyReplicated(partitionsMap, cluster, replicationFactors)

    optimiseForReplicaFairnessAcrossRacks(partitionsMap, cluster)
    optimiseForLeaderFairnessAcrossRacks(partitionsMap, cluster)

    optimiseForReplicaFairnessAcrossBrokers(partitionsMap, cluster, replicationFactors)
    optimiseForLeaderFairnessAcrossBrokers(partitionsMap, cluster)

    println("\nResult is:")
    print(partitionsMap, cluster)
    partitionsMap
  }

  /**
    * This method O(#under-replicated-partitions * #parititions) as we reevaluate the least loaded brokers for each under-replicated one we find
    * (could be optimised further but this seems a reasonable balance between simplicity and cost).
    */
  def ensureFullyReplicated(partitionsMap: mutable.Map[TopicAndPartition, scala.Seq[Int]], cluster: ReplicaFilter, replicationFactors: Map[String, Int]): Unit = {
    for (partition <- partitionsMap.keys) {
      def replicationFactor = replicationFactors.get(partition.topic).get
      def replicasForP = partitionsMap.get(partition).get

      while (replicasForP.size < replicationFactor) {
        val leastLoadedBrokers = cluster.leastLoadedBrokersPreferringOtherRacks(cluster.racksFor(partition))
        val leastLoadedValidBrokers = leastLoadedBrokers.filterNot(replicasForP.toSet).iterator

        val leastLoaded = leastLoadedValidBrokers.next
        partitionsMap.put(partition, replicasForP :+ leastLoaded)
        println(s"Additional replica was created on broker [$leastLoaded] for under-replicated partition [$partition].")
      }
    }
  }

  /**
    * This method is O(#unfair * #partitions) - this is now incorrect but does make me wonder how much faster the old way must have been!
    */
  def optimiseForReplicaFairnessAcrossRacks(partitionsMap: mutable.Map[TopicAndPartition, scala.Seq[Int]], cluster: ReplicaFilter) = {
    for (aboveParRack <- cluster.replicaFairness.aboveParRacks) {
      for (toMove <- cluster.weightedReplicasFor(aboveParRack)) {
        var moved = false
        for (belowParRack <- cluster.replicaFairness.belowParRacks) {
          for (brokerTo <- cluster.leastLoadedBrokerIds(belowParRack)) {
            if (cluster.obeysPartitionConstraint(toMove.partition, brokerTo) && moved == false) {
              move(toMove.partition, toMove.broker, brokerTo, partitionsMap)
              moved = true
            }
          }
        }
      }
    }
  }

  def optimiseForLeaderFairnessAcrossRacks(partitionsMap: mutable.Map[TopicAndPartition, scala.Seq[Int]], cluster: ReplicaFilter): Unit = {
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
  }

  def optimiseForReplicaFairnessAcrossBrokers(partitionsMap: mutable.Map[TopicAndPartition, scala.Seq[Int]], cluster: ReplicaFilter, replicationFactors: Map[String, Int]) = {
    /**
      * TODO refactor this to take the approach used in 2.1 - get list of most loaded replicas & least loaded brokers and move incrementally and ensuring there is enough supply and demand
      */
    var moved = false
    for (aboveParBroker <- cluster.replicaFairness.aboveParBrokers) {
      for (replicaToMove <- cluster.weightedReplicasFor(aboveParBroker)) {
        moved = false
        for (belowParBroker <- cluster.replicaFairness.belowParBrokers) {
          val partition = replicaToMove.partition
          val brokerFrom: Int = replicaToMove.broker
          val brokerTo: Int = belowParBroker.id
          if (cluster.obeysPartitionConstraint(partition, brokerTo) && moved == false) {
            if (cluster.obeysRackConstraint(partition, brokerFrom, brokerTo, replicationFactors)) {
              move(partition, brokerFrom, brokerTo, partitionsMap)
              moved = true
            }
          }
        }
      }
    }
  }

  def optimiseForLeaderFairnessAcrossBrokers(partitionsMap: mutable.Map[TopicAndPartition, scala.Seq[Int]], cluster: ReplicaFilter): Unit = {
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
  }

  def makeLeader(tp: TopicAndPartition, toPromote: Int, partitionsMap: collection.mutable.Map[TopicAndPartition, Seq[Int]]): Unit = {
    var replicas = partitionsMap.get(tp).get
    var currentLead = replicas(0)
    //remove old
    replicas = replicas.filter(_ != toPromote)
    //push first
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

  def print(partitionsMap: mutable.Map[TopicAndPartition, scala.Seq[Int]], cluster: ReplicaFilter): Unit = {
    println("\nPartitions to brokers: " + partitionsMap.map { case (k, v) => "\n" + k + " => " + v }.toSeq.sorted)
    println("\nBrokers to partitions: " + cluster.brokersToReplicas.map { x => "\n" + x._1.id + " : " + x._2.map("p" + _.partitionId) } + "\n")
  }
}
