package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection._


class MovesOptimisedRebalancePolicy extends RabalancePolicy with TopologyHelper with TopologyFactory {

  override def rebalancePartitions(brokers: Seq[BrokerMetadata], replicasForPartitions: Map[TopicAndPartition, Seq[Int]], replicationFactors: Map[String, Int]): Map[TopicAndPartition, Seq[Int]] = {
    val partitions = collection.mutable.Map(replicasForPartitions.toSeq: _*) //todo deep copy?
    val constraints: Constraints = new Constraints(brokers, partitions)
    print(partitions, brokers)

    //1. Ensure no under-replicated partitions
    fullyReplicated(partitions, constraints, replicationFactors, brokers)

    //2. Optimise across racks
    val view = new ByRack(brokers, partitions)
    replicaFairness(partitions, replicationFactors, view)
    leaderFairness(partitions, view)

    //3. Optimise brokers on each rack separately
    for (rack <- racks(brokers)) {
      val view = new ByBroker(brokers, partitions, rack)
      replicaFairness(partitions, replicationFactors, view)
      leaderFairness(partitions, new ByBroker(brokers, partitions, rack))
    }

    print(partitions, brokers)
    partitions
  }

  def fullyReplicated(partitions: mutable.Map[TopicAndPartition, Seq[Int]], cluster: Constraints, replicationFactors: Map[String, Int], brokers: Seq[BrokerMetadata]): Map[TopicAndPartition, Seq[Int]] = {
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
    partitions
  }

  def replicaFairness(partitionsMap: mutable.Map[TopicAndPartition, scala.Seq[Int]], replicationFactors: Map[String, Int], v: ClusterView): Unit = {
    var view = v

    val aboveParReplicas = v.replicasOnAboveParBrokers
    for (replicaFrom <- aboveParReplicas) {
      var moved = false
      for (brokerTo <- view.brokersWithBelowParReplicaCount) {
        val obeysPartition = view.constraints.obeysPartitionConstraint(replicaFrom.partition, brokerTo.id)
        val obeysRack = view.constraints.obeysRackConstraint(replicaFrom.partition, replicaFrom.broker, brokerTo.id, replicationFactors)

        if (!moved && obeysRack && obeysPartition) {
          move(replicaFrom.partition, replicaFrom.broker, brokerTo.id, partitionsMap)
          view = view.refresh(partitionsMap)
          moved = true
        }
      }
    }
  }

  def leaderFairness(partitions: mutable.Map[TopicAndPartition, scala.Seq[Int]], v: ClusterView): Unit = {
    var view = v

    val abParParts = view.leadersOnAboveParBrokers
    for (aboveParLeaderPartition <- abParParts) {
      var moved = false

      //Attempt to switch leadership within partitions to achieve fairness (i.e. no data movement)
      for (aboveParFollowerBrokerId <- partitions.get(aboveParLeaderPartition).get.drop(1)) {
        val brokersWithBelowParLeaders = view.brokersWithBelowParLeaderCount
        if (brokersWithBelowParLeaders.map(_.id).contains(aboveParFollowerBrokerId)) {
          //if so, switch leadership
          makeLeader(aboveParLeaderPartition, aboveParFollowerBrokerId, partitions)
          view = view.refresh(partitions)
          moved = true
        }
      }

      //If that didn't succeed, pick a replica from another partition, which is on a below par broker, and physically swap them around.
      if (!moved) {
        val aboveParLeaderBroker = partitions.get(aboveParLeaderPartition).get(0)
        for (broker <- view.brokersWithBelowParLeaderCount) {
          val followerReplicasOnBelowParBrokers = view.nonLeadReplicasFor(broker)
          for (belowParFollowerReplica <- followerReplicasOnBelowParBrokers) {
            val obeysPartitionOut = view.constraints.obeysPartitionConstraint(aboveParLeaderPartition, belowParFollowerReplica.broker)
            val obeysPartitionBack = view.constraints.obeysPartitionConstraint(belowParFollowerReplica.partition, aboveParLeaderBroker)

            if (!moved && obeysPartitionOut && obeysPartitionOut) {
              move(aboveParLeaderPartition, aboveParLeaderBroker, belowParFollowerReplica.broker, partitions)
              move(belowParFollowerReplica.partition, belowParFollowerReplica.broker, aboveParLeaderBroker, partitions)
              view = view.refresh(partitions)
              moved = true
            }
          }
        }
      }
    }
  }

  def makeLeader(tp: TopicAndPartition, toPromote: Int, partitionsMap: collection.mutable.Map[TopicAndPartition, Seq[Int]]): Unit = {
    var replicas = partitionsMap.get(tp).get
    var currentLead = replicas(0)

    if (toPromote != currentLead) {
      replicas = replicas.filter(_ != toPromote)
      replicas = Seq(toPromote) ++ replicas
      partitionsMap.put(tp, replicas)
      println(s"Leadership moved brokers: [$currentLead -> $toPromote] for partition $tp")
    }
    else println(s"Leadership change was not made as $toPromote was already the leader for partition $tp - see: ${partitionsMap.get(tp)}")
  }

  def move(tp: TopicAndPartition, from: Int, to: Int, partitionsMap: collection.mutable.Map[TopicAndPartition, Seq[Int]]): Unit = {
    def replaceFirst[A](a: Seq[A], repl: A, replwith: A): List[A] = a match {
      case Nil => Nil
      case head :: tail => if (head == repl) replwith :: tail else head :: replaceFirst(tail, repl, replwith)
    }

    if (to == from)
      println(s"Movement was not made as $to was already the broker $from")
    else {
      val replicas = replaceFirst(partitionsMap.get(tp).get, from, to)
      partitionsMap.put(tp, replicas)
    }
  }

  def print(partitionsMap: mutable.Map[TopicAndPartition, scala.Seq[Int]], brokers: Seq[BrokerMetadata]): Unit = {
    val brokersToReplicas = createBrokersToReplicas(brokers, brokers, partitionsMap)
    val brokersToLeaders = createBrokersToLeaders(brokers, brokers, partitionsMap)
    println("\nPartitions to brokers: " + partitionsMap.map { case (k, v) => "\n" + k + " => " + v }.toSeq.sorted)
    println("\nBrokers to replicas: " + brokersToReplicas.map { x => "\n" + x._1.id + " : " + x._2.map("p" + _.partitionId) } + "\n")
    println("\nBrokers to leaders: " + brokersToLeaders.map { x => "\n" + x._1.id + " - size:" + x._2.size } + "\n")
    println("\nRacks to replica Counts " + getRackReplicaCounts(brokersToReplicas))
    println("\nRacks to leader Counts " + getRackLeaderCounts(brokersToLeaders))
    println("\nBroker to replica Counts " + getBrokerReplicaCounts(brokersToReplicas).map { case (k, v) => (k.id, v) })
    println("\nBroker to leader Counts " + getBrokerLeaderCounts(brokersToLeaders))
  }

  def print(partitionsMap: mutable.Map[TopicAndPartition, scala.Seq[Int]]): Unit = {
    println("\nPartitions to brokers: " + partitionsMap.map { case (k, v) => "\n" + k + " => " + v }.toSeq.sorted)
  }
}