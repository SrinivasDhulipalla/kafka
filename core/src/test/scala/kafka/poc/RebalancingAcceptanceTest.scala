package kafka.poc

import kafka.poc.Helper._
import org.junit.Assert._
import org.junit.Test

class RebalancingAcceptanceTest {

  @Test
  def shouldRebalanceAwkwardlyArrangedCluster(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given 2 topics, with rep-factor 2 and 2 topics with rep-factor 3, all with 100 partitions loaded on first few brokers
    val brokerCount = 10
    val partitionCount = 10
    val replicas = Seq(100, 101, 102)
    val topics = Seq("t1", "t2", "t3", "t4")
    val replicaCount = replicas.size

    val reps = topics.map((_, replicaCount)).toMap
    val brokers = (100 until (100 + brokerCount / 2)).map(bk(_, "rack1")) ++ ((100 + brokerCount/2) until (100 + brokerCount)).map(bk(_, "rack2"))

    //TODO refactor me
    val partitions = ((0 until partitionCount).map(p(_, "t1") -> replicas).toMap
      ++ (0 until partitionCount).map(p(_, "t2") -> replicas).toMap
      ++ (0 until partitionCount).map(p(_, "t3") -> replicas).toMap
      ++ (0 until partitionCount).map(p(_, "t4") -> replicas).toMap)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, reps)

    //Then counts shoudl match
    assertEquals(topics.size * partitionCount, reassigned.size)
    assertEquals(topics.size * partitionCount * replicaCount, reassigned.values.flatten.size)

    //Then replicas should be evenly spread
    for (brokerId <- 100 until (100 + brokerCount)) {
      val expected: Int = topics.size * partitionCount * replicaCount / brokerCount
      assertEquals(expected, reassigned.values.flatten.filter(_ == brokerId).size)
    }

    //Then leaders should be evenly spread
    for (brokerId <- 100 until (100 + brokerCount)) {
      val expected: Int = partitionCount * topics.size / brokerCount
      assertEquals(expected, reassigned.values.map(_ (0)).filter(_ == brokerId).size)
    }
  }
}