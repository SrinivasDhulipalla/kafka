package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection._


class MovesOptimisedRebalancePolicy extends RabalancePolicy with TopologyHelper with TopologyFactory {


  /**
    * Plan:
    * Push fullyReplicated to use it's own dedicated view??
    * Push byRack and byBroker refreshable where byBroker takes a rack as arguemnt and byRack doesn't. That sorts out the constructor problem.
    */

  override def rebalancePartitions(brokers: Seq[BrokerMetadata], replicasForPartitions: Map[TopicAndPartition, Seq[Int]], replicationFactors: Map[String, Int]): Map[TopicAndPartition, Seq[Int]] = {
    val partitions = collection.mutable.Map(replicasForPartitions.toSeq: _*) //todo deep copy?
    println("\nBrokers: " + brokers.map { b => "\n" + b })
    print(partitions, brokers)




    //1. Ensure no under-replicated partitions
    val constraints: Constraints = new Constraints(brokers, partitions)
    fullyReplicated(partitions, constraints, replicationFactors, brokers)

    //2. Optimise Racks
    println("\nOptimising replica fairness over racks\n")
    val view: ByRack = new ByRack(brokers, partitions)
    replicaFairness(partitions, view.constraints, replicationFactors, view)

    println("\nEarly-Result is:")
    print(partitions, brokers)

    println("\nOptimising leader fairness over racks\n")
    leaderFairness(partitions, new ByRack(brokers, partitions))

    println("\nMid-Result is:")
    print(partitions, brokers)

    //3. Optimise brokers on each byRack
    for (rack <- racks(brokers)) {
      println("\nOptimising Replica Fairness over brokers for rack " + rack + "\n")
      print(partitions, brokers)
      val view: ByBroker = new ByBroker(brokers, partitions, rack)
      replicaFairness(partitions, view.constraints, replicationFactors, view)
      println("\nOptimising Leader Fairness over brokers for rack " + rack + "\n")
      print(partitions, brokers)
      leaderFairness(partitions, new ByBroker(brokers, partitions, rack))
    }

    println("\nResult is:")
    print(partitions, brokers)
    partitions
  }

  /**
    * This method O(#under-replicated-partitions * #parititions) as we reevaluate the least loaded brokers for each under-replicated one we find
    * (could be optimised further but this seems a reasonable balance between simplicity and cost).
    */
  def fullyReplicated(partitions: mutable.Map[TopicAndPartition, Seq[Int]], cluster: Constraints, replicationFactors: Map[String, Int], brokers: Seq[BrokerMetadata]): Unit = {
    val brokersToReplicas: scala.Seq[(BrokerMetadata, scala.Seq[Replica])] = createBrokersToReplicas(brokers, brokers, partitions)

    for (partition <- partitions.keys) {
      def replicationFactor = replicationFactors.get(partition.topic).get
      def replicasForP = partitions.get(partition).get
      val racks = racksFor(partition, brokers, partitions)

      while (replicasForP.size < replicationFactor) {
        val leastLoadedBrokers = leastLoadedBrokersPreferringOtherRacks(brokersToReplicas, brokers, racks)
        val leastLoadedValidBrokers = leastLoadedBrokers.filterNot(replicasForP.toSet).iterator

        val leastLoaded = leastLoadedValidBrokers.next
        partitions.put(partition, replicasForP :+ leastLoaded)
        println(s"Additional replica was created on broker [$leastLoaded] for under-replicated partition [$partition].")
      }
    }
  }

  def replicaFairness(partitionsMap: mutable.Map[TopicAndPartition, scala.Seq[Int]], constraints: RebalanceConstraints, replicationFactors: Map[String, Int], v: ClusterView): Unit = {

    var view = v
    val aboveParReplicas = v.aboveParReplicas
    println("aboveParReplicas-main: " + aboveParReplicas)
    for (replicaFrom <- aboveParReplicas) {
      var moved = false
      val belowParBrokers: scala.Seq[BrokerMetadata] = view.belowParBrokers
      println("belowParBrokers-main: " + belowParBrokers)
      for (brokerTo <- belowParBrokers) {
        val obeysPartition: Boolean = constraints.obeysPartitionConstraint(replicaFrom.partition, brokerTo.id)
        val obeysRack: Boolean = constraints.obeysRackConstraint(replicaFrom.partition, replicaFrom.broker, brokerTo.id, replicationFactors)
        if (obeysPartition && obeysRack && moved == false) {
          move(replicaFrom.partition, replicaFrom.broker, brokerTo.id, partitionsMap)
          view = view.refresh(partitionsMap)
          moved = true
        } else
          println(s"Failed to move ${replicaFrom.partition} [${replicaFrom.broker}] => [${brokerTo.id}] due to rack[${obeysRack}] or partition [${obeysPartition}] constraint")

      }
    }
  }

  def leaderFairness(partitions: mutable.Map[TopicAndPartition, scala.Seq[Int]], v: ClusterView): Unit = {

    var view = v

    val abParParts = view.aboveParPartitions
    println("abParParts: " + abParParts)
    for (aboveParPartition <- abParParts) {
      //not sure if i need this...
      if (view.aboveParPartitions().contains(aboveParPartition)) {
        //check to see if the partition has a non-leader replica on below par racks
        for (nonLeadReplicas <- partitions.get(aboveParPartition).get.drop(1)) {
          if (view.brokersWithBelowParLeaders.contains(nonLeadReplicas)) {
            //if so, switch leadership
            makeLeader(aboveParPartition, nonLeadReplicas, partitions)
            view = view.refresh(partitions)
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
    def replaceFirst[A](a: Seq[A], repl: A, replwith: A): List[A] = a match {
      case Nil => Nil
      case head :: tail => if (head == repl) replwith :: tail else head :: replaceFirst(tail, repl, replwith)
    }

    val replicas = replaceFirst(partitionsMap.get(tp).get, from, to)

    partitionsMap.put(tp, replicas)
    println(s"Partition $tp was moved from broker [$from] to [$to]")
  }

  def print(partitionsMap: mutable.Map[TopicAndPartition, scala.Seq[Int]], brokers: Seq[BrokerMetadata]): Unit = {
    println("\nPartitions to brokers: " + partitionsMap.map { case (k, v) => "\n" + k + " => " + v }.toSeq.sorted)
    val view = new ByRack(brokers, partitionsMap)
    println("\nBrokers to replicas: " + view.brokersToReplicas.map { x => "\n" + x._1.id + " : " + x._2.map("p" + _.partitionId) } + "\n")
    println("\nBrokers to leaders: " + view.brokersToLeaders.map { x => "\n" + x._1.id + " - size:" + x._2.size } + "\n")
  }

  def print(partitionsMap: mutable.Map[TopicAndPartition, scala.Seq[Int]]): Unit = {
    println("\nPartitions to brokers: " + partitionsMap.map { case (k, v) => "\n" + k + " => " + v }.toSeq.sorted)
  }

}
