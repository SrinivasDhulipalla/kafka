package kafka.poc


import kafka.admin.BrokerMetadata
import kafka.poc.Helper._
import org.junit.Assert._
import org.junit.Test

import scala.collection.Seq

class ReplicaFilterTest {

  @Test
  def shouldCreateSimpleClusterTopologyOfBrokersToReplicas(): Unit = {
    val brokers = List(bk(100, "rack1"), bk(101, "rack2"))
    val partitions = Map(
      p(0) -> List(100, 101),
      p(1) -> List(100))

    val topology = new ReplicaFilter(brokers, partitions).brokersToReplicas

    val expected = Map(
      new BrokerMetadata(101, Option("rack2")) -> Seq(new Replica(topic, 0, 101)),
      new BrokerMetadata(100, Option("rack1")) -> Seq( new Replica(topic, 0, 100),new Replica(topic, 1, 100))
    )

    assertEquals(expected.toString(), topology.toMap.toString()) //TODO how do a do deep comparision without toString?
  }

  @Test
  def shouldSummariseReplicaCounts(): Unit = {
    val brokers = List(bk(100, "rack1"), bk(101, "rack2"))
    val partitions = Map(
      p(0) -> List(100, 101),
      p(1) -> List(100))

    val counts = new ReplicaFilter(brokers, partitions).replicaFairness.brokerReplicaCounts

    val expected = Map(
      new BrokerMetadata(101, Option("rack2")) -> 1,
      new BrokerMetadata(100, Option("rack1")) -> 2
    )

    assertEquals(expected.toString(), counts.toMap.toString()) //TODO how do a do deep comparision without toString?
  }


  @Test
  def shouldCalculateRackFairValue(): Unit ={
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack2"), bk(103, "rack2"))

    //1 replica, 2 racks
    assertEquals(0, new ReplicaFilter(brokers, Map(
      p(0) -> List(103))).replicaFairness.rackFairReplicaValue.toInt)

    //2 replicas, 2 racks
    assertEquals(1, new ReplicaFilter(brokers, Map(
      p(0) -> List(103, 102))).replicaFairness.rackFairReplicaValue.toInt)

    //3 replicas, 2 racks
    assertEquals(1, new ReplicaFilter(brokers, Map(
      p(0) -> List(103, 102, 101))).replicaFairness.rackFairReplicaValue.toInt)

    //4 replicas, 2 racks
    assertEquals(2, new ReplicaFilter(brokers, Map(
      p(0) -> List(103, 102, 101, 100))).replicaFairness.rackFairReplicaValue.toInt)

  }

  @Test
  def shouldOrderBrokersByReplicaLoad(): Unit = {
    //Given three brokers with increasing load: 102,101,100
    val brokers = (100 to 102).map(bk(_, "rack1"))
    val partitions = Map(
      p(0) -> List(100, 101, 102),
      p(1) -> List(100, 101, 102),
      p(2) -> List(100, 101, 102),
      p(3) -> List(100, 101),
      p(4) -> List(100))

    //When
    val leastLoaded = new ReplicaFilter(brokers, partitions).leastLoadedBrokerIds()

    //Then
    assertEquals(Seq(100, 101, 102), leastLoaded)
  }

  @Test
  def shouldConsiderEmptyBrokers(): Unit = {
    //Given two brokers, only one with replicas
    val brokers = (100 to 101).map(bk(_, "rack1"))
    val partitions = Map(p(4) -> List(101))

    //When
    val leastLoaded = new ReplicaFilter(brokers, partitions).leastLoadedBrokerIds()

    //Then
    assertEquals(Seq(101, 100), leastLoaded)
  }


  @Test
  def shouldDownrankRacksSoTheyAppearLast(): Unit = {
    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack2"), bk(103, "rack2"))
    val partitions = Map(
      p(0) -> List(103, 102, 101, 100),
      p(1) -> List(103, 102, 101),
      p(2) -> List(103, 102),
      p(3) -> List(103))

    //When
    val leastLoaded = new ReplicaFilter(brokers, partitions).leastLoadedBrokersDownranking(Seq("rack1"))

    //Then least loaded would be 103, 102, 101, 100 but with down-ranking rack1 should get:
    assertEquals(Seq(101, 100, 103, 102), leastLoaded)
  }

  @Test
  def shouldAllowReplicaMoveIfDoesNotBreakRackConstraint(): Unit ={

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack2"))
    val partitions = Map(
      p(0) -> List(100, 100)
    )

    //When
    val brokerFrom: Int = 100
    val brokerTo: Int = 101
    val cluster: ReplicaFilter = new ReplicaFilter(brokers, partitions)

    //Then
    assertEquals(true, cluster.obeysRackConstraint(p(0), brokerFrom, brokerTo,  r(2)))
  }

  @Test
  def shouldFailIfPartitionMoveBreaksRackConstraint(): Unit ={

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack2"))
    val partitions = Map(
      p(0) -> List(100, 101)
    )

    //When
    val brokerFrom: Int = 100
    val brokerTo: Int = 101
    val cluster: ReplicaFilter = new ReplicaFilter(brokers, partitions)

    //Then
    assertEquals(false, cluster.obeysRackConstraint(p(0), brokerFrom, brokerTo,  r(2)))
  }

}
