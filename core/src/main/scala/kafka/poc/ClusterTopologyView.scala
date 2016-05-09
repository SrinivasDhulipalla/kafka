package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc.fairness.{LeaderFairness, ReplicaFairness, Fairness}

import scala.collection._
import scala.collection.mutable.LinkedHashMap


class ClusterTopologyView(b: Seq[BrokerMetadata], p: Map[TopicAndPartition, Seq[Int]]) {

  var partitions: Map[TopicAndPartition, Seq[Int]] = p
  var brokers = b
  var rack = ""
  var brokersToReplicas = brokersToReplicasRun(b,b,p)
  var brokersToLeaders = brokersToLeadersRun(b,b,p)

  def setRack(r: String) = {
    rack = r
    refreshPartitionsView(b, p, r)
  }

  def refreshPartitionsView(b: Seq[BrokerMetadata], p: Map[TopicAndPartition, Seq[Int]], rack: String): Unit = {
    println("refreshed partition view with new partitions " + p)


    if (rack.length > 0) {
      println("refreshing racks to rack " + rack)
      brokers = b.filter(_.rack.get == rack)
      partitions = filter(rack, b, p)

      brokersToReplicas = brokersToReplicasRun(b,brokers, p).filter(_._1.rack.get == rack)
      brokersToLeaders = brokersToLeadersRun(b, brokers,p).filter(_._1.rack.get == rack)
    }else{
      brokersToReplicas = brokersToReplicasRun(b, b,p)
      brokersToLeaders = brokersToLeadersRun(b,b, p)
    }

    replicaFairness = new ReplicaFairness(brokersToReplicas, rackCount)
    leaderFairness = new LeaderFairness(brokersToLeaders)


    println("paritions " + partitions)

    //could be that this doesn't work because partitions can have replicas spread on brokers on different racks making fair value off.


    println("upper level broker leader counts " + brokersToLeaders)
  }


  def filter(rack: String, brokers: scala.Seq[BrokerMetadata], partitions: Map[TopicAndPartition, scala.Seq[Int]]): Map[TopicAndPartition, scala.Seq[Int]] = {
    def bk(id: Int): BrokerMetadata = brokers.filter(_.id == id).last

    val foo = partitions.map { case (p, replicas) => (p, replicas.filter(bk(_).rack.get == rack)) }
      .filter { case (p, replicas) => replicas.size > 0 }
    foo
  }

  def brokersToReplicasRun(allBrokers: Seq[BrokerMetadata], relevantBrokers: Seq[BrokerMetadata], partitions: Map[TopicAndPartition, Seq[Int]]): Seq[(BrokerMetadata, Seq[Replica])] = {

    def bk(id: Int): BrokerMetadata =  allBrokers.filter(_.id == id).last

    val existing = partitions
      .map { case (tp, replicas) => (tp, replicas.map(new Replica(tp.topic, tp.partition, _))) } //enrich replica object
      .values
      .flatMap(replica => replica) //list of all replicas
      .groupBy(replica => replica.broker) //group by broker to create: broker->[Replica]
      .toSeq
      .sortBy(_._2.size) //sort by highest replica count
      .map { x => (bk(x._1), x._2.toSeq) } //turn broker id into BrokerMetadata

    val emptyBrokers = relevantBrokers.filterNot(existing.map(_._1).toSet)
      .map(x => (x, Seq.empty[Replica]))

    emptyBrokers ++ existing
  }

  def brokersToLeadersRun(allBrokers: Seq[BrokerMetadata],relevantBrokers: Seq[BrokerMetadata], partitions: Map[TopicAndPartition, Seq[Int]]): Seq[(BrokerMetadata, Iterable[TopicAndPartition])] = {

    def bk(id: Int): BrokerMetadata =  {
      allBrokers.filter(_.id == id).last
    }

    val existing = partitions
        .filter(_._2.size > 0)
      .map { case (tp, replicas) => (tp, (tp, bk(replicas(0)))) }.values //convert to tuples: [TopicAndPartition,BrokerMetadata]
      .groupBy(_._2) //group by brokers to create: Broker -> [TopicAndPartition]
      .toSeq
      .sortBy(_._2.size)
      .map { case (x, y) => (x, y.map(x => x._1)) }

    val emptyBrokers = relevantBrokers.filterNot(existing.map(_._1).toSet)
      .map(x => x -> Iterable.empty[TopicAndPartition])

    emptyBrokers ++ existing
  }

  def brokersToLeadersMap(): Map[BrokerMetadata, Iterable[TopicAndPartition]] = {
    brokersToLeaders.toMap
  }


  var replicaFairness = new ReplicaFairness(brokersToReplicas, rackCount)
  var leaderFairness = new LeaderFairness(brokersToLeaders)
  val byRack = new ByRack()
  val byBroker = new ByBroker()

  class ByRack extends ClusterView {
    def aboveParReplicas(): scala.Seq[Replica] = replicaFairness.aboveParRacks.flatMap(weightedReplicasFor(_))

    def belowParBrokers(): scala.Seq[BrokerMetadata] = replicaFairness.belowParRacks.flatMap(leastLoadedBrokerIds(_))

    def aboveParPartitions(): Seq[TopicAndPartition] = leaderFairness.aboveParRacks().flatMap(leadersOn(_))

    def brokersWithBelowParLeaders(): scala.Seq[Int] = brokersOn(leaderFairness.belowParRacks())

    def refresh(pNew: Map[TopicAndPartition, Seq[Int]]): Unit = {
      refreshPartitionsView(b, pNew, rack)
    }
  }

  class ByBroker extends ClusterView {
    def aboveParReplicas(): scala.Seq[Replica] = {
      val foo = replicaFairness.aboveParBrokers.flatMap(weightedReplicasFor(_))
      println("replicaFairness.aboveParBrokers " + replicaFairness.aboveParBrokers)
      println("aboveParReplicas: " + foo)
      foo
    }

    def belowParBrokers(): scala.Seq[BrokerMetadata] = replicaFairness.belowParBrokers

    def aboveParPartitions(): scala.Seq[TopicAndPartition] = leaderFairness.aboveParBrokers().flatMap(leadersOn(_))

    def brokersWithBelowParLeaders(): scala.Seq[Int] = leaderFairness.belowParBrokers().map(_.id)

    def refresh(pNew: Map[TopicAndPartition, Seq[Int]]): Unit = {
      refreshPartitionsView(b, pNew, rack)
    }
  }


  def rackCount: Int = {
    brokersToReplicas.map(_._1.rack).distinct.size
  }

  def racks: Seq[String] = {
    brokersToReplicas.map(_._1.rack.get).distinct
  }

  def leastLoadedBrokerIds(): Seq[Int] = {
    brokersToReplicas.map(_._1.id).reverse
  }

  def replicaExists(replica: Any, rack: String): Boolean = {
    brokersToReplicas.filter(_._1.rack.get == rack).map(_._2).size > 0
  }

  object constraints extends RebalanceConstraints {
    def obeysRackConstraint(partition: TopicAndPartition, brokerFrom: Int, brokerTo: Int, replicationFactors: Map[String, Int]): Boolean = {
      val minRacksSpanned = Math.min(replicationFactors.get(partition.topic).get, rackCount)

      //get replicas for partition, replacing brokerFrom with brokerTo
      var proposedReplicas: scala.Seq[Int] = partitions.get(partition).get
      val index: Int = proposedReplicas.indexOf(brokerFrom)
      proposedReplicas = proposedReplicas.patch(index, Seq(brokerTo), 1)

      //find how many racks are now spanned
      val racksSpanned = proposedReplicas.map(bk(_)).map(_.rack).distinct.size

      racksSpanned >= minRacksSpanned
    }

    def obeysPartitionConstraint(replica: TopicAndPartition, brokerMovingTo: Int): Boolean = {
      !replicasFor(brokerMovingTo).map(_.partition).contains(replica)
    }
  }

  /**
    * Find the least loaded brokers, but push those on the supplied racks to the bottom of the list.
    *
    * Then least loaded would be 103, 102, 101, 100 (based on replica count with least loaded last)
    * but rack1 (100, 101) should drop in priority so we should get:
    *
    * The least loaded broker will be returned first
    *
    */
  def leastLoadedBrokersPreferringOtherRacks(racks: Seq[String]): Iterable[Int] = {
    downrank(brokersOn(racks), leastLoadedBrokerIds()).reverse
  }

  def racksFor(p: TopicAndPartition): Seq[String] = {
    brokers.filter(broker =>
      partitions.get(p).get
        .contains(broker.id)
    ).map(_.rack.get)
  }

  private def leastLoadedBrokerIds(rack: String): Seq[BrokerMetadata] = {
    brokersToReplicas.map(_._1).reverse
      .filter(broker => broker.rack.get == rack)
  }

  private def downrank(toDownrank: scala.Seq[Int], all: scala.Seq[Int]): scala.Seq[Int] = {
    val notDownranked = all.filterNot(toDownrank.toSet)
    val downranked = all.filter(toDownrank.toSet)

    downranked ++ notDownranked
  }

  private def brokersOn(racks: Seq[String]): scala.Seq[Int] = {
    brokers.filter(broker => racks.contains(broker.rack.get)).map(_.id)
  }

  private def leadersOn(rack: String): Seq[TopicAndPartition] = {
    brokersToLeaders
      .filter(_._1.rack.get == rack)
      .map(_._2)
      .flatMap(x => x)
  }

  private def leadersOn(broker: BrokerMetadata): Seq[TopicAndPartition] = {
    brokersToLeaders //TODO should probably be a map lookup
      .filter(_._1 == broker)
      .map(_._2).last.toSeq
  }

  private def weightedReplicasFor(rack: String): Seq[Replica] = {
    //TODO implement weighting later - for now just return replicas in rack in any order
    //TODO2 we need to interleave these results by broker see MovesOptimisedRebalancePolicyTest.providesPotentiallyUnexpectedResult
    brokersToReplicas.filter(_._1.rack.get == rack).sortBy(_._2.size).map(_._2).flatten
  }

  private def weightedReplicasFor(broker: BrokerMetadata): Seq[Replica] = {
    //TODO implement weighting later - for now just return replicas in rack in any order
    //TODO2 we need to interleave these results by broker see MovesOptimisedRebalancePolicyTest.providesPotentiallyUnexpectedResult to
    brokersToReplicas.filter(_._1 == broker).sortBy(_._2.size).map(_._2).flatten
  }

  private def replicasFor(broker: Int): Seq[Replica] = {
    brokersToReplicas.filter(_._1.id == broker).seq(0)._2
  }

  private def bk(id: Int): BrokerMetadata = {
    //    println("id:"+id)
    brokers.filter(_.id == id).last
  }
}

