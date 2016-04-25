package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection._
import scala.collection.mutable.LinkedHashMap


class ReplicaFilter(brokers: Seq[BrokerMetadata], partitions: Map[TopicAndPartition, Seq[Int]]) {

  //Look up full broker metadata object for id
  def bk(id: Int): BrokerMetadata = {
    brokers.filter(_.id == id).last
  }

  def brokersToReplicas: Seq[(BrokerMetadata, Seq[Replica])] = {
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

  def brokersToLeaders: Seq[(BrokerMetadata, Iterable[TopicAndPartition])] = {
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

  def leastLoadedBrokers(): Seq[BrokerMetadata] = {
    brokersToReplicas.map(_._1).reverse
  }

  def leastLoadedBrokerIds(rack: String): Seq[Int] = {
    val leastLoaded = leastLoadedBrokers
      .filter(broker => broker.rack.get == rack)
      .map(_.id)
    println("least loaded for rack " + rack + " is " + leastLoaded)
    leastLoaded
  }

  //find the least loaded brokers, but push those on the supplied racks to the bottom of the list.
  def leastLoadedBrokersDownranking(racks: Seq[String]): Iterable[Int] = {
    downrank(brokersOn(racks), leastLoadedBrokerIds())
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

  def partitionsFor(racks: Seq[String]): Seq[TopicAndPartition] = {
    brokersToLeaders
      .filter(x => racks.contains(x._1.rack.get))
      .map(_._2)
      .flatMap(x => x)
  }

  def leadersOn(rack: String): Seq[TopicAndPartition] = {
    brokersToLeaders
      .filter(_._1.rack.get == rack)
      .map(_._2)
      .flatMap(x => x)
  }

  def leadersOn(broker: BrokerMetadata): Seq[TopicAndPartition] = {
    brokersToLeaders //TODO should probably be a map lookup
      .filter(_._1 == broker)
      .map(_._2).last.toSeq
  }

  def weightedReplicasFor(rack: String): Seq[Replica] = {
    //TODO implement weighting later - for now just return replicas in rack in any order
    brokersToReplicas.filter(_._1.rack.get == rack).map(_._2).flatMap(x => x)
  }

  def weightedReplicasFor(broker: BrokerMetadata): Seq[Replica] = {
    //TODO implement weighting later - for now just return replicas in rack in any order
    brokersToReplicas.filter(_._1 == broker).map(_._2).flatMap(x => x)
  }

  def replicaExists(replica: Any, rack: String): Boolean = {
    brokersToReplicas.filter(_._1.rack.get == rack).map(_._2).size > 0
  }

  def replicasFor(broker: BrokerMetadata): Seq[Replica] = {
    brokersToReplicas.filter(_._1 == broker).seq(0)._2
  }
  def replicasFor(broker: Int): Seq[Replica] = {
    brokersToReplicas.filter(_._1.id == broker).seq(0)._2
  }

  object replicaFairness {
    //Summarise the topology as BrokerMetadata -> ReplicaCount
    def brokerReplicaCounts() = LinkedHashMap(
      brokersToReplicas
        .map { case (x, y) => (x, y.size) }
        .sortBy(_._2)
        : _*
    )

    def rackReplicaCounts() = LinkedHashMap(
      brokersToReplicas
        .map { case (x, y) => (x, y.size) }
        .groupBy(_._1.rack.get)
        .mapValues(_.map(_._2).sum)
        .toSeq
        .sortBy(_._2)
        : _*
    )

    //Define rackFairValue: floor(replica-count / rack-count) replicas
    def rackFairReplicaValue() = Math.floor(
      brokerReplicaCounts.values.sum /
        rackCount
    )

    //Define  floor(replica-count / broker-count) replicas
    def brokerFairReplicaValue() = Math.floor(
      brokerReplicaCounts.values.sum /
        brokerReplicaCounts
          .keys.toSeq.distinct.size
    )

    def countFromPar(rack: String): Int = {
      Math.abs(rackReplicaCounts.get(rack).get - rackFairReplicaValue.toInt)
    }

    def countFromPar(broker: BrokerMetadata): Int = {
      Math.abs(brokerReplicaCounts.get(broker).get - brokerFairReplicaValue.toInt)
    }

    def aboveParRacks(): Seq[String] = {
      //return racks for brokers where replica count is over fair value
      rackReplicaCounts
        .filter(_._2 > rackFairReplicaValue)
        .keys
        .toSeq.distinct
    }

    def belowParRacks(): Seq[String] = {
      //return racks for brokers where replica count is over fair value
      rackReplicaCounts
        .filter(_._2 < rackFairReplicaValue)
        .keys
        .toSeq
        .distinct
    }

    def aboveParBrokers(): Seq[BrokerMetadata] = {
      //return racks for brokers where replica count is over fair value
      brokerReplicaCounts
        .filter(_._2 > brokerFairReplicaValue)
        .keys.toSeq.distinct
    }

    def belowParBrokers(): Seq[BrokerMetadata] = {
      //return racks for brokers where replica count is over fair value
      brokerReplicaCounts
        .filter(_._2 < brokerFairReplicaValue)
        .keys.toSeq.distinct
    }
  }

  object leaderFairness {
    def brokerLeaderPartitionCounts() = LinkedHashMap(
      brokersToLeaders
        .map { case (x, y) => (x, y.size) }
        .sortBy(_._2)
        : _*
    )

    def rackLeaderPartitionCounts: Map[String, Int] = {
      brokersToLeaders
        .map { case (x, y) => (x, y.size) }
        .groupBy(_._1.rack.get)
        .mapValues(_.map(_._2).sum)
    }


    def rackFairLeaderValue() = {
      Math.floor(partitions.size / rackCount)
    }

    def brokerFairLeaderValue() = {
      Math.floor(partitions.size / brokersToReplicas.size)
    }

    def aboveParRacks(): Seq[String] = {
      rackLeaderPartitionCounts
        .filter(_._2 > rackFairLeaderValue)
        .keys
        .toSeq
        .distinct
    }

    def belowParRacks(): Seq[String] = {
      rackLeaderPartitionCounts
        .filter(_._2 < rackFairLeaderValue)
        .keys
        .toSeq
        .distinct
    }

    def aboveParBrokers(): Seq[BrokerMetadata] = {
      brokerLeaderPartitionCounts
        .filter(_._2 > brokerFairLeaderValue)
        .keys.toSeq.distinct
    }

    def belowParBrokers(): Seq[BrokerMetadata] = {
      brokerLeaderPartitionCounts
        .filter(_._2 < brokerFairLeaderValue)
        .keys.toSeq.distinct
    }
  }

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
