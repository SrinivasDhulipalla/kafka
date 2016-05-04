package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc.fairness.{LeaderFairness, ReplicaFairness, Fairness}

import scala.collection._
import scala.collection.mutable.LinkedHashMap


class ClusterTopologyView(brokers: Seq[BrokerMetadata], partitions: Map[TopicAndPartition, Seq[Int]]) {

  val replicaFairness = new ReplicaFairness(brokersToReplicas, rackCount)
  val leaderFairness = new LeaderFairness(brokersToLeaders, partitions.size, brokers.size, rackCount)

  object byRack extends ClusterView {
    def aboveParReplicas(): scala.Seq[Replica] = replicaFairness.aboveParRacks.flatMap(weightedReplicasFor(_))

    def belowParBrokers(): scala.Seq[BrokerMetadata] = replicaFairness.belowParRacks.flatMap(leastLoadedBrokerIds(_))

    def aboveParLeaders(): Seq[TopicAndPartition] = leaderFairness.aboveParRacks().flatMap(leadersOn(_))

    def brokersWithBelowParLeaders(): scala.Seq[Int] = brokersOn(leaderFairness.belowParRacks())
  }

  object byBroker extends ClusterView  {
    def aboveParReplicas(): scala.Seq[Replica] = replicaFairness.aboveParBrokers.flatMap(weightedReplicasFor(_))

    def belowParBrokers(): scala.Seq[BrokerMetadata] = replicaFairness.belowParBrokers

    def aboveParLeaders(): scala.Seq[TopicAndPartition] = leaderFairness.aboveParBrokers().flatMap(leadersOn(_))

    def brokersWithBelowParLeaders(): scala.Seq[Int] = leaderFairness.belowParBrokers().map(_.id)
  }

  def brokersToReplicas(): Seq[(BrokerMetadata, Seq[Replica])] = {
    val existing = partitions
      .map { case (tp, replicas) => (tp, replicas.map(new Replica(tp.topic, tp.partition, _))) } //enrich replica object
      .values
      .flatMap(replica => replica) //list of all replicas
      .groupBy(replica => replica.broker) //group by broker to create: broker->[Replica]
      .toSeq
      .sortBy(_._2.size) //sort by highest replica count
      .map { x => (bk(x._1), x._2.toSeq) } //turn broker id into BrokerMetadata

    val emptyBrokers = brokers.filterNot(existing.map(_._1).toSet)
      .map(x => (x, Seq.empty[Replica]))

    emptyBrokers ++ existing
  }

  def brokersToLeaders(): Seq[(BrokerMetadata, Iterable[TopicAndPartition])] = {
    val existing = partitions
      .map { case (tp, replicas) => (tp, (tp, bk(replicas(0)))) }.values //convert to tuples: [TopicAndPartition,BrokerMetadata]
      .groupBy(_._2) //group by brokers to create: Broker -> [TopicAndPartition]
      .toSeq
      .sortBy(_._2.size)
      .map { case (x, y) => (x, y.map(x => x._1)) }

    val emptyBrokers = brokers.filterNot(existing.map(_._1).toSet)
      .map(x => x -> Iterable.empty[TopicAndPartition])

    emptyBrokers ++ existing
  }

  def rackCount: Int = {
    brokersToReplicas.map(_._1.rack).distinct.size
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
    brokersToReplicas.filter(_._1.rack.get == rack).map(_._2).flatMap(x => x)
  }

  private def weightedReplicasFor(broker: BrokerMetadata): Seq[Replica] = {
    //TODO implement weighting later - for now just return replicas in rack in any order
    brokersToReplicas.filter(_._1 == broker).map(_._2).flatMap(x => x)
  }

  private def replicasFor(broker: Int): Seq[Replica] = {
    brokersToReplicas.filter(_._1.id == broker).seq(0)._2
  }

  private def bk(id: Int): BrokerMetadata = {
    brokers.filter(_.id == id).last
  }
}

