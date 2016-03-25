package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection._
import scala.collection.mutable.LinkedHashMap


class ReplicaFilter(brokers: Seq[BrokerMetadata], partitions: Map[TopicAndPartition, Seq[Int]]) {
  val partitionsCopy = partitions

  //Look up full broker metadata object for id
  def bk(id: Int): BrokerMetadata = {
    brokers.filter(_.id == id).last
  }

  //Map of BrokerMetadata -> Seq[Replicas]
  def brokerToReplicasByMostLoaded = {

    //Group replicas by broker, sorting by the number of replicas (most loaded broker first)
    //... enriching with Replica & BrokerMetadata classes on the way
    val brokerToReplicaExistingReplicas = LinkedHashMap(
      partitions
        .map { case (tp, replicas) => (tp, replicas.map(new Replica(tp.topic, tp.partition, _))) }
        .values
        .flatMap(replica => replica)
        .groupBy(replica => replica.broker)
        .toSeq
        .sortBy(_._2.size)
        : _*
    ).map { case (k, v) => (bk(k), v) }


    //Include empty brokers too, if there are any
    val emptyBrokers = LinkedHashMap(
      brokers
        .filterNot(brokerToReplicaExistingReplicas.keys.toSet)
        .map(x => x -> Seq.empty[Replica])
        : _*)

    //Merge the two lists so the empty brokers come before the most loaded list
    //TODO there must be a better way of doing this. Concatanating works but Intelij doesn't like it :(
    val brokerToReplicas = new LinkedHashMap[BrokerMetadata, Seq[Replica]]()
    for (kv <- emptyBrokers)
      brokerToReplicas.put(kv._1, kv._2)
    for (kv <- brokerToReplicaExistingReplicas)
      brokerToReplicas.put(kv._1, kv._2.toSeq)

    brokerToReplicas
  }

  //Map of BrokerMetadata (Broker) -> Seq[TopicPartitions aka Leaders]
  def brokerToLeaderPartitionsByMostLoaded = {

    //seems a bit obtuse. We should be able to remove thihs final map
    //      .map{case(x,y)=>(x, y.)}


    val existing = LinkedHashMap(
      partitions
        .map { case (tp, replicas) => (tp, (tp, bk(replicas(0)))) } //convert map to list tuples
        .values
        .groupBy(_._2) //group by brokers
        .toSeq
        .sortBy(_._2.size)
        : _*
    ).map { case (x, y) => (x, y.map(x => x._1)) }


    //Include empty brokers too, if there are any
    val emptyBrokers = LinkedHashMap(
      brokers
        .filterNot(existing.keys.toSet)
        .map(x => x -> Seq.empty[TopicAndPartition])
        : _*)

    //Merge the two lists so the empty brokers come before the most loaded list
    //TODO there must be a better way of doing this. Concatanating works but Intelij doesn't like it :(
    val brokersToLeaderPartitions = new LinkedHashMap[BrokerMetadata, Seq[TopicAndPartition]]()
    for (kv <- emptyBrokers)
      brokersToLeaderPartitions.put(kv._1, kv._2)
    for (kv <- existing)
      brokersToLeaderPartitions.put(kv._1, kv._2.toSeq)

    println("brokersToLeaderPartitions: " + brokersToLeaderPartitions)

    brokersToLeaderPartitions
  }

  //Summarise the topology as BrokerMetadata -> ReplicaCount
  def brokerReplicaCounts() = LinkedHashMap(
    brokerToReplicasByMostLoaded
      .map { case (x, y) => (x, y.size) }
      .toSeq
      .sortBy(_._2)
      : _*
  )

  def brokerLeaderPartitionCounts() = LinkedHashMap(
    brokerToLeaderPartitionsByMostLoaded
      .map { case (x, y) => (x, y.size) }
      .toSeq
      .sortBy(_._2)
      : _*
  )

  //Define rackFairValue: floor(replica-count / rack-count) replicas
  def rackFairReplicaValue() = Math.floor(
    brokerReplicaCounts.values.sum /
      brokerReplicaCounts
        .keys
        .map(_.rack)
        .toSeq.distinct.size
  )

  //Define rackFairLeaderValue: floor(leader-count / broker-count) leaders
  def rackFairLeaderValue() = Math.floor(
    partitions.size /
      brokerToReplicasByMostLoaded.size
  )


  def leastLoadedBrokers(): Seq[Int] = {
    mostLoadedBrokers().toSeq.reverse
  }

  def leastLoadedBrokers(rack: String): Seq[Int] = {
    leastLoadedBrokers().filter(brokerId => brokers.find(_.id == brokerId).get.rack.get == rack)
  }

  def mostLoadedBrokers(): Iterable[Int] = {
    brokerToReplicasByMostLoaded.keySet.toSeq.map(_.id)
  }

  def mostLoadedBrokersDownrankingRacks(racks: Seq[String]): Iterable[Int] = {
    downrank(brokersOn(racks), mostLoadedBrokers().toSeq)
  }

  def leastLoadedBrokersDownranking(racks: Seq[String]): Iterable[Int] = {
    downrank(brokersOn(racks), leastLoadedBrokers())
  }

  private def downrank(toDownrank: scala.Seq[Int], all: scala.Seq[Int]): scala.Seq[Int] = {
    val notDownranked = all.filterNot(toDownrank.toSet)
    val downranked = all.filter(toDownrank.toSet)

    downranked ++ notDownranked
  }

  def brokersOn(racks: Seq[String]): scala.Seq[Int] = {
    brokers.filter(broker => racks.contains(broker.rack.get)).map(_.id)
  }

  def racksFor(p: TopicAndPartition): Seq[String] = {
    brokers.filter(broker =>
      partitions.get(p).get
        .contains(broker.id)
    ).map(_.rack.get)
  }

  def aboveParRacks(): Seq[String] = {
    //return racks for brokers where replica count is over fair value
    brokerReplicaCounts
      .filter(_._2 > rackFairReplicaValue)
      .keys
      .map(_.rack.get)
      .toSeq.distinct
  }

  object leaderFairness {
    def aboveParRacks(): Seq[String] = {
      println("brokerLeaderPartitionCounts: " + brokerLeaderPartitionCounts)
      //todo clean this duplication up
      brokerLeaderPartitionCounts
        .filter(_._2 > rackFairLeaderValue)
        .keys
        .map(_.rack.get)
        .toSeq
        .distinct
    }

    def belowParRacks(): Seq[String] = {
      println("brokerLeaderPartitionCounts: " + brokerLeaderPartitionCounts)
      //todo clean this duplication up
      brokerLeaderPartitionCounts
        .filter(_._2 < rackFairLeaderValue)
        .keys
        .map(_.rack.get)
        .toSeq
        .distinct
    }
  }

  def belowParRacks(): Seq[String] = {
    //return racks for brokers where replica count is over fair value
    brokerReplicaCounts
      .filter(_._2 < rackFairReplicaValue)
      .keys
      .map(_.rack.get)
      .toSeq
      .distinct
  }

  def partitionsFor(rack: String): Seq[TopicAndPartition] = {
    brokerToLeaderPartitionsByMostLoaded
      .filter(_._1.rack == rack)
      .values
      .flatMap(x => x)
      .toSeq
  }

  def partitionsFor(racks: Seq[String]): Seq[TopicAndPartition] = {
    brokerToLeaderPartitionsByMostLoaded
      .filter(x => racks.contains(x._1.rack))
      .values
      .flatMap(x => x)
      .toSeq
  }

  def leadersOn(rack: String): Seq[TopicAndPartition] = {
    brokerToLeaderPartitionsByMostLoaded
      .filter(_._1.rack.get == rack)
      .values
      .flatMap(x => x)
      .toSeq
  }

  def weightedReplicasFor(rack: String): Seq[Replica] = {
    //TODO implement weighting later - for now just return replicas in rack in any order

    brokerToReplicasByMostLoaded.filter(_._1.rack.get == rack).values.flatMap(x => x).toSeq
  }

  def replicaExists(replica: Any, rack: String): Boolean = {
    brokerToReplicasByMostLoaded.filter(_._1.rack.get == rack).values.size > 0
  }
}


class Replica(val topic: String, val partition: Int, val broker: Int) {
  def topicAndPartition(): TopicAndPartition = {
    new TopicAndPartition(topic, partition)
  }

  override def toString = s"Replica[$topic:$partition:$broker]"


  def canEqual(other: Any): Boolean = other.isInstanceOf[Replica]

  override def equals(other: Any): Boolean = other match {
    case that: Replica =>
      (that canEqual this) &&
        topic == that.topic &&
        partition == that.partition &&
        broker == that.broker
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(topic, partition, broker)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
