package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection._


class MovesOptimisedRebalancePolicy extends RabalancePolicy {

  override def rebalancePartitions(brokers: Seq[BrokerMetadata], replicasForPartitions: Map[TopicAndPartition, Seq[Int]], replicationFactors: Map[String, Int]): Map[TopicAndPartition, Seq[Int]] = {
    val partitionsMap = collection.mutable.Map(replicasForPartitions.toSeq: _*) //todo deep copy?
    val cluster = new ReplicaFilter(brokers, partitionsMap)
    println(partitionsMap)
    println(cluster.brokersToReplicas.map{x=> "\n" + x._1.id + " : " + x._2.map("p"+_.partition)})

    /**
      * Step 1: Ensure partitions are fully replicated
      */
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

    /**
      * Step 2.1: Optimise for replica fairness across racks
      */
    //Get the most loaded set of replicas from above par racks
    val aboveParReplicas = cluster.replicaFairness.aboveParRacks()
      .flatMap { rack => cluster.weightedReplicasFor(rack).take(
        cluster.replicaFairness.countFromPar(rack))
      }

    //get the least loaded brokers
    val belowParOpenings = cluster.replicaFairness.belowParRacks()
      .flatMap { rack => cluster.leastLoadedBrokerIds(rack).take(cluster.replicaFairness.countFromPar(rack)) }

    //only move if there is supply and demand
    val moves = Math.min(aboveParReplicas.size, belowParOpenings.size) - 1

    for (i <- (0 to moves)) {
      val brokerFrom: Int = aboveParReplicas(i).broker
      val brokerTo: Int = belowParOpenings(i)
      move(aboveParReplicas(i).topicAndPartition, brokerFrom, brokerTo, partitionsMap)
    }


    /**
      * Step 2.2: Optimise for leader fairness across racks
      */
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

    /**
      * Step 3.1: Optimise for replica fairness across brokers
      */
    var moved = false
    for (aboveParBroker <- cluster.replicaFairness.aboveParBrokers) {
      for (replicaToMove <- cluster.weightedReplicasFor(aboveParBroker)) {
        moved = false
        for (belowParBroker <- cluster.replicaFairness.belowParBrokers) {
          val partition = replicaToMove.topicAndPartition
          val brokerFrom: Int = replicaToMove.broker
          val brokerTo: Int = belowParBroker.id
          //if obeys partition constraint
          if (!cluster.replicasFor(brokerTo).map(_.topicAndPartition).contains(partition) && moved == false) {
            //todo add maintains rack constraint
            move(partition, brokerFrom, brokerTo, partitionsMap)
            moved = true
          }
        }
      }
    }



    //    //Get the most loaded set of replicas from above par brokers
    //    val abovePar = cluster.replicaFairness.aboveParBrokers()
    //      .flatMap {broker => cluster.weightedReplicasFor(broker).take(
    //        cluster.replicaFairness.countFromPar(broker))}
    //    println("above par "+abovePar)
    //
    //    //get the least loaded brokers
    //    val belowPar = cluster.replicaFairness.belowParBrokers()
    //    println("below par "+belowPar)
    //
    //    //only move if there is supply and demand
    //    var moves2 = Math.min(abovePar.size, belowPar.size)
    //    var index = 0
    //    while (index < moves2) {
    //      val brokerFrom = abovePar(index).broker
    //      val brokerTo = belowPar(index)
    //      //for each partition on brokersFrom.
    //      for(brokerTo <- belowPar){
    //        if(!cluster.replicasFor(brokerTo).contains(brokerFrom)) {
    //          move(abovePar(index).topicAndPartition, brokerFrom, brokerTo.id, partitionsMap)
    //          moves2 -= 1
    //        }
    //      }
    //
    //      index += 1
    //    }


    /**
      * Step 3.2: Optimise for leader fairness across brokers
      */
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
}
