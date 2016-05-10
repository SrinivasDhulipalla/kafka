package kafka.poc

import kafka.poc.Helper._
import org.junit.Assert._
import org.junit.Test

class FairnessTest {

  @Test
  def shouldCalculateRackFairValue(): Unit ={
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack2"), bk(103, "rack2"))

    //1 replica, 2 racks
    assertEquals(0, new ByRack(brokers, Map(
      p(0) -> List(103))).replicaFairness.rackFairReplicaValue.toInt)

    //2 replicas, 2 racks
    assertEquals(1, new ByRack(brokers, Map(
      p(0) -> List(103, 102))).replicaFairness.rackFairReplicaValue.toInt)

    //3 replicas, 2 racks
    assertEquals(1, new ByRack(brokers, Map(
      p(0) -> List(103, 102, 101))).replicaFairness.rackFairReplicaValue.toInt)

    //4 replicas, 2 racks
    assertEquals(2, new ByRack(brokers, Map(
      p(0) -> List(103, 102, 101, 100))).replicaFairness.rackFairReplicaValue.toInt)

  }
}
