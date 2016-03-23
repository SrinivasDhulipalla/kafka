package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection._

class ReplicaFilter(brokers: Seq[BrokerMetadata], assignment: Map[TopicAndPartition, Seq[Int]]) {

  println(assignment)


  def mostLoadedBrokers(): Iterable[Int] = {
    val brokerToReplicaCounts: mutable.LinkedHashMap[Int, Int] = mutable.LinkedHashMap(
      assignment.values
        .flatMap(x => x)
        .groupBy(x => x)
        .map { case (x, y) => (x, y.size) }
        .toSeq
        .sortBy(_._2)
        : _*
    )
    val mostLoaded = brokerToReplicaCounts.keys
    val emptyBrokers = brokers.map(_.id).filterNot(mostLoaded.toSet)

    println("brokerToReplicaCounts " + brokerToReplicaCounts)
    println("most loaded " + mostLoaded)
    println("empty brokers " + emptyBrokers)
    println("MostLoaded, combined " + (emptyBrokers.toSeq ++ mostLoaded.toSeq))

    emptyBrokers.toSeq ++ mostLoaded.toSeq
  }

  def mostLoadedBrokersDownrankingRacks(racks: Seq[String]): Iterable[Int] = {
    downrank(racks, mostLoadedBrokers().toSeq)
  }

  def leastLoadedBrokersDownranking(racks: Seq[String]): Iterable[Int] = {
    downrank(racks, mostLoadedBrokers().toSeq.reverse)
  }

  private def downrank(racks: scala.Seq[String], brokers: scala.Seq[Int]): scala.Seq[Int] = {
    println("downrannking all brokers "+brokers)
    val brokersDownranked = brokersOn(racks)
    println("brokers on downranked racks "+ racks+" mapped to "+brokersDownranked)
    val notDownranked = brokers.filterNot(brokersDownranked.toSet)
    val downranked = brokers.filter(brokersDownranked.toSet)
    println(("notDownranked " + notDownranked))
    println(("downranked " + downranked))
    downranked ++ notDownranked
  }

  private def brokersOn(racks: Seq[String]): scala.Seq[Int] = {
    brokers.filter(broker => racks.contains(broker.rack.get)).map(_.id)
  }


}
