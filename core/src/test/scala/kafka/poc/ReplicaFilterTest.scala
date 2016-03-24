package kafka.poc


import kafka.poc.Helper._
import org.junit.Assert._
import org.junit.Test

import scala.collection.Seq

class ReplicaFilterTest {

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
    val mostLoaded = new ReplicaFilter(brokers, partitions).mostLoadedBrokers()

    //Then
    assertEquals(Seq(102, 101, 100), mostLoaded)
  }

  @Test
  def shouldConsiderEmptyBrokers(): Unit = {
    //Given two brokers, only one with replicas
    val brokers = (100 to 101).map(bk(_, "rack1"))
    val partitions = Map(p(4) -> List(101))

    //When
    val mostLoaded = new ReplicaFilter(brokers, partitions).mostLoadedBrokers()

    //Then
    assertEquals(Seq(100, 101), mostLoaded)
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
    val mostLoaded = new ReplicaFilter(brokers, partitions).mostLoadedBrokersDownrankingRacks(Seq("rack2"))

    //Then most loaded would be 100, 101, 102, 103 but with down-ranking rack2 should get:
    assertEquals(Seq(102, 103, 100, 101), mostLoaded)

    //When
    val leastLoaded = new ReplicaFilter(brokers, partitions).leastLoadedBrokersDownranking(Seq("rack1"))

    //Then least loaded would be 103, 102, 101, 100 but with down-ranking rack1 should get:
    assertEquals(Seq(101, 100, 103, 102), leastLoaded)
  }

//  @Test
//  def shouldCalculateRackFairValue(): Unit ={
//    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack2"), bk(103, "rack2"))
//
//    assertEquals(0, new ReplicaFilter(brokers, Map(
//      p(0) -> List(103))).rackFairValue.toInt)
//
//    assertEquals(0, new ReplicaFilter(brokers, Map(
//      p(0) -> List(103, 102))).rackFairValue.toInt)
//
//    assertEquals(0, new ReplicaFilter(brokers, Map(
//      p(0) -> List(103, 102, 101))).rackFairValue.toInt)
//
//    assertEquals(1, new ReplicaFilter(brokers, Map(
//      p(0) -> List(103, 102, 101, 100))).rackFairValue.toInt)
//
//  }

}
