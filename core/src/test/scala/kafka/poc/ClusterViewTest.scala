package kafka.poc


import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import kafka.poc.Helper._
import kafka.poc.fairness.ReplicaFairness
import org.junit.Assert._
import org.junit.Test

import scala.collection.Seq

class ClusterViewTest {

  @Test
  def shouldCreateSimpleClusterTopologyOfBrokersToReplicas(): Unit = {
    val brokers = List(bk(100, "rack1"), bk(101, "rack2"))
    val partitions = Map(
      p(0) -> List(100, 101),
      p(1) -> List(100))

    val topology = new ByRack(brokers, partitions).brokersToReplicas

    val expected = Map(
      new BrokerMetadata(101, Option("rack2")) -> Seq(new Replica(topic, 0, 101)),
      new BrokerMetadata(100, Option("rack1")) -> Seq( new Replica(topic, 0, 100),new Replica(topic, 1, 100))
    )

    assertEquals(expected.toString(), topology.toMap.toString()) //TODO how do a do deep comparision without toString?
  }

  @Test
  def shouldSummariseLeaderCounts(): Unit = {
    val brokers = List(bk(100, "rack1"), bk(101, "rack2"))
    val partitions = Map(
      p(0) -> List(100, 101),
      p(1) -> List(100, 101),
      p(2) -> List(101, 100),
      p(3) -> List(101, 100),
      p(4) -> List(101, 100)
    )

    val filter = new ByRack(brokers, partitions)
    val leaderCounts = filter.brokersToLeaders.toMap

    println(leaderCounts)
    assertEquals(2, leaderCounts.get(bk(100, "rack1")).get.size)
    assertEquals(3, leaderCounts.get(bk(101, "rack2")).get.size)
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
    val view = new ByRack(brokers, partitions)
    val leastLoaded = view.leastLoadedBrokerIds(view.brokersToReplicas)

    //Then
    assertEquals(Seq(100, 101, 102), leastLoaded)
  }

  @Test
  def shouldConsiderEmptyBrokers(): Unit = {
    //Given two brokers, only one with replicas
    val brokers = (100 to 101).map(bk(_, "rack1"))
    val partitions = Map(p(4) -> List(101))

    //When
    val view = new ByRack(brokers, partitions)
    val leastLoaded = view.leastLoadedBrokerIds(view.brokersToReplicas)

    //Then
    assertEquals(Seq(101, 100), leastLoaded)
  }


  @Test
  def shouldPreferRacksThatAreNotPassedSoTheyAppearLast(): Unit = {
    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack2"), bk(103, "rack2"))
    val partitions = Map(
      p(0) -> List(103, 102, 101, 100),
      p(1) -> List(103, 102, 101),
      p(2) -> List(103, 102),
      p(3) -> List(103))

    //When
    val view = new ByRack(brokers, partitions)
    val leastLoaded = view.leastLoadedBrokersPreferringOtherRacks(view.brokersToReplicas, brokers, Seq("rack1"))

    //Then least loaded would be 100, 101, 102, 103 (based purely on replica count, least loaded first)
    //but rack1 (100, 101) should drop in priority so they appear last:
    assertEquals(Seq(102, 103, 100, 101), leastLoaded)
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
    val cluster: Constraints = new Constraints(brokers, partitions)

    //Then
    assertTrue(cluster.obeysRackConstraint(p(0), brokerFrom, brokerTo,  r(2)))
  }



  @Test
  def shouldAllowReplicaCreationIfDoesNotBreakRackConstraint(): Unit ={

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack2"))
    val partitions = Map(
      p(0) -> List(100)
    )
    val replicationFactor= r(2)

    //When
    val brokerFrom: Int = -1 //doesn't exist (i.e. we're under-replicated
    val brokerTo: Int = 101

    val cluster: Constraints = new Constraints(brokers, partitions)
    //Then
    assertTrue( cluster.obeysRackConstraint(p(0), brokerFrom, brokerTo,  replicationFactor))
  }


  @Test
  def shouldFailIfReplicaCreationBreaksRackConstraint(): Unit ={

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack2"))
    val partitions = Map(
      p(0) -> List(100)
    )

    //When
    val brokerFrom: Int = -1 //doesn't exist (i.e. we're under-replicated
    val brokerTo: Int = 100
    val cluster: Constraints = new Constraints(brokers, partitions)

    //Then
    assertEquals(false, cluster.obeysRackConstraint(p(0), brokerFrom, brokerTo,  r(2)))
  }


  @Test
  def shouldFailPartitionConstraintIfReplicaAlreadyExistsOnTargetForMove(): Unit ={
    //Given
    val brokers = List(bk(100, "rack1"))
    val partitions = Map(
      p(0) -> List(100)
    )

    val cluster: Constraints = new Constraints(brokers, partitions)
    assertEquals(false, cluster.obeysPartitionConstraint(p(0), 100))
  }

  @Test
  def shouldPassPartitionConstraintIfReplicaDoesntExistsOnTargetForMove(): Unit ={
    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"))
    val partitions = Map(
      p(0) -> List(100)
    )

    val cluster: Constraints = new Constraints(brokers, partitions)
    assertEquals(true, cluster.obeysPartitionConstraint(p(0), 101))
  }
}
