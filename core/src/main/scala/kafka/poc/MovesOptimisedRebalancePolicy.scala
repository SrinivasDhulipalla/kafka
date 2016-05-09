package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection._


class MovesOptimisedRebalancePolicy extends RabalancePolicy {



  /**
    * Euston we have a problem:
    * We need to optimise broker on each rack individually. The tempting approach here
    * is to simply divide the cluster up at a 'parititions' input stage. This might work
    * but i'm worried if we chop the cluster up we'll get odd results as some replicas will
    * be in other racks. This might not matter. check constraints etc.
    */
  override def rebalancePartitions(brokers: Seq[BrokerMetadata], replicasForPartitions: Map[TopicAndPartition, Seq[Int]], replicationFactors: Map[String, Int]): Map[TopicAndPartition, Seq[Int]] = {
    val partitions = collection.mutable.Map(replicasForPartitions.toSeq: _*) //todo deep copy?
    println("\nBrokers: " + brokers.map { b => "\n" + b })
    val cluster = new ClusterTopologyView(brokers, partitions)
    print(partitions, cluster)

    //1. Ensure no under-replicated partitions
    fullyReplicated(partitions, cluster, replicationFactors)

    //2. Optimise Racks
    println("\nOptimising racks")
    replicaFairness(partitions, cluster.constraints, replicationFactors, cluster.byRack)
    leaderFairness(partitions, cluster.byRack)

    println("\nMid-Result is:")
    print(partitions, cluster)

    //3. Optimise brokers on each byRack
    val brokerView = new ClusterTopologyView(brokers, partitions)
    for (rack <- cluster.racks) {
      println("\nOptimising Replica Fairness over brokers for rack "+rack+ "\n")
      brokerView.setRack(rack)
      replicaFairness(partitions, cluster.constraints, replicationFactors, brokerView.byBroker)
      println("\nOptimising Leader Fairness over brokers for rack "+rack+ "\n")
      print(partitions, cluster)
      leaderFairness(partitions, brokerView.byBroker)
    }

    println("\nResult is:")
    print(partitions, cluster)
    partitions
  }

  /**
    * This method O(#under-replicated-partitions * #parititions) as we reevaluate the least loaded brokers for each under-replicated one we find
    * (could be optimised further but this seems a reasonable balance between simplicity and cost).
    */
  def fullyReplicated(partitionsMap: mutable.Map[TopicAndPartition, Seq[Int]], cluster: ClusterTopologyView, replicationFactors: Map[String, Int]): Unit = {
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

  def replicaFairness(partitionsMap: mutable.Map[TopicAndPartition, scala.Seq[Int]], constraints: RebalanceConstraints, replicationFactors: Map[String, Int], view: ClusterView) = {
    val aboveParReplicas: scala.Seq[Replica] = view.aboveParReplicas
    println("aboveParReplicas-main: " + aboveParReplicas)
    for (replicaFrom <- aboveParReplicas) {
      var moved = false
      val belowParBrokers: scala.Seq[BrokerMetadata] = view.belowParBrokers
      println("belowParBrokers-main: " + belowParBrokers)
      for (brokerTo <- belowParBrokers) {
        if (constraints.obeysPartitionConstraint(replicaFrom.partition, brokerTo.id) && moved == false) {
          if (constraints.obeysRackConstraint(replicaFrom.partition, replicaFrom.broker, brokerTo.id, replicationFactors)) {
            move(replicaFrom.partition, replicaFrom.broker, brokerTo.id, partitionsMap)
            view.refresh(partitionsMap)
            moved = true
          }
        }
      }
    }
  }

  def leaderFairness(partitions: mutable.Map[TopicAndPartition, scala.Seq[Int]], view: ClusterView): Unit = {
    val abParParts: scala.Seq[TopicAndPartition] = view.aboveParPartitions
    println("abParParts: "+abParParts)
    for (aboveParPartition <- abParParts) {
      //not sure if i need this...
      if (view.aboveParPartitions().contains(aboveParPartition)) {
        //check to see if the partition has a non-leader replica on below par racks
        for (nonLeadReplicas <- partitions.get(aboveParPartition).get.drop(1)) {
          if (view.brokersWithBelowParLeaders.contains(nonLeadReplicas)) {
            //if so, switch leadership
            makeLeader(aboveParPartition, nonLeadReplicas, partitions)
            view.refresh(partitions)
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

  def print(partitionsMap: mutable.Map[TopicAndPartition, scala.Seq[Int]], cluster: ClusterTopologyView): Unit = {
    println("\nPartitions to brokers: " + partitionsMap.map { case (k, v) => "\n" + k + " => " + v }.toSeq.sorted)
    println("\nBrokers to partitions: " + cluster.brokersToReplicas.map { x => "\n" + x._1.id + " : " + x._2.map("p" + _.partitionId) } + "\n")
  }
}
