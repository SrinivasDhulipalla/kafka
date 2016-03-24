package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.Predef
import scala.collection._


class ReplicaFilter(brokers: Seq[BrokerMetadata], partitions: Map[TopicAndPartition, Seq[Int]]) {

  private val metadataToReplicas1: mutable.LinkedHashMap[BrokerMetadata, scala.Iterable[Replica]] = mutable.LinkedHashMap(
    partitions
      .map { case (tp, replicas) => (tp, replicas.map(new Replica(tp.topic, tp.partition, _))) }
      .values
      .flatMap(replica => replica)
      .groupBy(replica => replica.broker)
      .toSeq
      .sortBy(_._2.size)
      : _*
  ).map { case (k, v) => (brokers.filter(_.id == k).last, v) }
  //this is BrokerMetadata to Replica
  private var brokerToReplicas = metadataToReplicas1

  println("before empties " + brokerToReplicas)
  private val emptyBrokers = mutable.LinkedHashMap(brokers.filterNot(brokerToReplicas.keys.toSet).map(x => x -> Seq.empty[Replica]): _*)
  //  brokerToReplicas = emptyBrokers ++ brokerToReplicas

  val foo = new mutable.LinkedHashMap[BrokerMetadata, scala.Iterable[Replica]]()
  for (kv <- emptyBrokers) {
    foo.put(kv._1, kv._2)
  }
  for (kv <- metadataToReplicas1) {
    foo.put(kv._1, kv._2)
  }

  brokerToReplicas = foo


  println("after empties " + brokerToReplicas)
  println("......")
  for (x <- brokerToReplicas) {
    println(x)
  }
  println("......")

  //.map(_, Seq())

  //  println("after empties "+brokerToReplicas)

  //this is BrokerMetadata to Count
  private val brokerReplicaCounts = mutable.LinkedHashMap(
    brokerToReplicas
      .map { case (x, y) => (x, y.size) }
      .toSeq
      .sortBy(_._2)
      : _*
  )


  //floor(replica-count / rack-count) replicas
  private val rackFairValue = Math.floor(
    brokerReplicaCounts.values.sum / brokerReplicaCounts
      .keys
      .map(_.rack)
      .toSeq.distinct.size)


  def leastLoadedBrokers(): Seq[Int] = {
    mostLoadedBrokers().toSeq.reverse
  }

  def leastLoadedBrokers(rack: String): Seq[Int] = {
    leastLoadedBrokers().filter(brokerId => brokers.find(_.id == brokerId).get.rack == rack)
  }


  def mostLoadedBrokers(): Iterable[Int] = {
    brokerToReplicas.keySet.toSeq.map(_.id)
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

    println("downrannking all " + all)
    println(("downranked " + downranked))
    println(("notDownranked " + notDownranked))
    println(("combined " + (downranked ++ notDownranked)))

    downranked ++ notDownranked
  }

  private def brokersOn(racks: Seq[String]): scala.Seq[Int] = {
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
      .filter(_._2 > rackFairValue)
      .keys
      .map(_.rack.get)
      .toSeq.distinct
  }

  def belowParRacks(): Seq[String] = {
    //return racks for brokers where replica count is over fair value
    brokerReplicaCounts.filter(_._2 < rackFairValue).keys.map(_.rack.get).toSeq.distinct
  }


  def weightedReplicasFor(rack: String): Seq[Replica] = {
    //TODO implement weighting later - for now just return replicas in rack in any order

    brokerToReplicas.filter(_._1.rack == rack).values.flatMap(x => x).toSeq
  }

  def replicaExists(replica: Any, rack: String): Boolean = {
    brokerToReplicas.filter(_._1.rack == rack).values.size > 0
  }

}


class Replica(val topic: String, val partition: Int, val broker: Int) {
  def topicAndPartition(): TopicAndPartition = {
    new TopicAndPartition(topic, partition)
  }

  override def toString = s"Replica[$topic:$partition:$broker]"
}
