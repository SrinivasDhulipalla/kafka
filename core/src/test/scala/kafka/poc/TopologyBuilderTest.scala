package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.poc.Helper._
import org.junit.Test
import org.junit.Assert._
class TopologyBuilderTest {

  @Test
  def shouldFindNonLeadReplicas(): Unit = {
    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack2"), bk(103, "rack2"))
    val partitions = Map(
      p(0, "t1") -> List(100, 101, 102),
      p(1, "t2") -> List(100, 101, 102),
      p(2, "t3") -> List(100, 101, 102),
      p(3, "t4") -> List(100, 101, 102))
    val reps = Map("t1" -> 4, "t2" -> 4, "t3" -> 2, "t4" -> 2)

    val nonLeaders = new TopologyFactory{}.createBrokersToNonLeaders(brokers, brokers, partitions)

    print(nonLeaders.map { x => "\n" + x._1.id + " : " + x._2.map("p" + _.partitionId) })
    assertEquals(0, nonLeaders.toMap.get(bk(100,"rack1")).get.size)
    assertEquals(4, nonLeaders.toMap.get(bk(101,"rack1")).get.size)
    assertEquals(4, nonLeaders.toMap.get(bk(102,"rack2")).get.size)
    assertEquals(0, nonLeaders.toMap.get(bk(103,"rack2")).get.size)

  }
}
