package kafka.poc

import kafka.poc.Helper._
import org.junit.Assert._
import org.junit.Test

class RebalancingAcceptanceTest {

  @Test
  def shouldRebalanceSmallFourTopicClusterSpreadOverTwoRacks(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()
    val broker0: Int = 100

    //Given
    val brokerCount = 100
    val partitionCount = 100
    val replicas = Seq(100, 101, 102)
    val topics = Seq("t1", "t2", "t3", "t4")

    val reps = topics.map((_, replicas.size)).toMap
    val brokers = ((broker0 until broker0 + brokerCount / 2).map(bk(_, "rack1"))
      ++ ((broker0 + brokerCount / 2) until (broker0 + brokerCount)).map(bk(_, "rack2")))

    val partitions = topics.map { topic => (0 until partitionCount).map(p(_, topic) -> replicas).toMap }.flatten.toMap

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, reps)

    //Then counts should match
    assertEquals(topics.size * partitionCount, reassigned.size)
    assertEquals(topics.size * partitionCount * replicas.size, reassigned.values.flatten.size)

    //Then replicas should be evenly spread
    for (brokerId <- 100 until (100 + brokerCount)) {
      val expected: Int = topics.size * partitionCount * replicas.size / brokerCount
      assertEquals(expected, reassigned.values.flatten.filter(_ == brokerId).size)
    }

    //Then leaders should be evenly spread
    for (brokerId <- 100 until (100 + brokerCount)) {
      val expected: Int = partitionCount * topics.size / brokerCount
      assertEquals(expected, reassigned.values.map(_ (0)).filter(_ == brokerId).size)
    }
  }

  @Test
  def shouldRebalanceClusterWithDifferentSizedTopicsAndReplicaCountsButSingleRack(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()
    val broker0: Int = 100

    //Given
    val brokerCount = 91
    val partitionCount = 71
    val replicas1 = Seq(100, 101, 102)
    val replicas2 = Seq(100, 101)
    val topics1 = Seq("t1", "t2")
    val topics2 = Seq("t5", "t6")
    val allTopics = topics1 ++ topics2


    val reps = topics1.map((_, replicas1.size)).toMap ++ topics2.map((_, replicas2.size)).toMap
    val brokers = ((broker0 until broker0 + brokerCount / 2).map(bk(_, "rack1"))
      ++ ((broker0 + brokerCount / 2) until (broker0 + brokerCount)).map(bk(_, "rack1")))

    val partitions = (topics1.map { topic => (0 until partitionCount).map(p(_, topic) -> replicas1).toMap }.flatten.toMap
      ++ topics2.map { topic => (0 until partitionCount).map(p(_, topic) -> replicas2).toMap }.flatten.toMap)

    println("Brokers" + brokers)
    println("partitions" + partitions)
    println("reps" + reps)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, reps)

    val expectedNumPartitions = allTopics.size * partitionCount
    val expectedNumReplicas = topics1.size * partitionCount * replicas1.size + topics2.size * partitionCount * replicas2.size

    //Then counts should match
    assertEquals(expectedNumPartitions, reassigned.size)
    assertEquals(expectedNumReplicas, reassigned.values.flatten.size)

    //Then replicas should be evenly spread
    for (brokerId <- broker0 until (broker0 + brokerCount)) {
      val expectedLowerBound: Int = Math.floor(expectedNumReplicas.toFloat / brokerCount).toInt
      val actual: Int = reassigned.values.flatten.filter(_ == brokerId).size
      assertTrue(s"expected:$expectedLowerBound/+1 actual:$actual", expectedLowerBound == actual || expectedLowerBound + 1 == actual)
    }

    //Then leaders should be evenly spread
    for (brokerId <- broker0 until (broker0 + brokerCount)) {
      val expectedLowerBound: Int = Math.floor(expectedNumPartitions / brokerCount).toInt
      val actual: Int = reassigned.values.map(_ (0)).filter(_ == brokerId).size
      assertTrue(s"expected:$expectedLowerBound/+1 actual:$actual", expectedLowerBound == actual || expectedLowerBound + 1 == actual)
    }
  }

  @Test //totally broken, multiple racks throws it off for some reason
  def shouldRebalanceClusterWithDifferentSizedTopicsAndReplicaCountsAndRacks(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()
    val broker0: Int = 100

    //Given
    val brokerCount = 171
    val partitionCount = 139
    val replicas1 = Seq(100, 101, 102)
    val replicas2 = Seq(100, 101)
    val topics1 = Seq("t1", "t2")
    val topics2 = Seq("t5", "t6")
    val allTopics = topics1 ++ topics2


    val reps = topics1.map((_, replicas1.size)).toMap ++ topics2.map((_, replicas2.size)).toMap
    val brokers = ((broker0 until broker0 + brokerCount / 2).map(bk(_, "rack1"))
      ++ ((broker0 + brokerCount / 2) until (broker0 + brokerCount)).map(bk(_, "rack2")))

    val partitions = (topics1.map { topic => (0 until partitionCount).map(p(_, topic) -> replicas1).toMap }.flatten.toMap
      ++ topics2.map { topic => (0 until partitionCount).map(p(_, topic) -> replicas2).toMap }.flatten.toMap)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, reps)



    val expectedNumPartitions = allTopics.size * partitionCount
    val expectedNumReplicas = topics1.size * partitionCount * replicas1.size + topics2.size * partitionCount * replicas2.size

    //Then counts should match
    assertEquals(expectedNumPartitions, reassigned.size)
    assertEquals(expectedNumReplicas, reassigned.values.flatten.size)

    //Then replicas should be evenly spread per rack (it is possible for there to be more spread across racks)
    for (rack <- Seq("rack2", "rack1")) {
      var min = -1
      var max = -1
      for (broker <- brokers.filter(_.rack.get == rack)) {
        val actual: Int = reassigned.values.flatten.filter(_ == broker.id).size
        if (actual > max || max == -1)
          max = actual
        if (actual < min || min == -1)
          min = actual
      }
      println(s"$rack: max:$max, min:$min")
      assertTrue(Seq(0, 1).contains(max - min))
    }


    //Then leaders should be evenly spread
    for (brokerId <- broker0 until (broker0 + brokerCount)) {
      val expectedLeaders: Int = Math.floor(expectedNumPartitions / brokerCount).toInt
      val actual: Int = reassigned.values.map(_ (0)).filter(_ == brokerId).size
      assertTrue(s"expected:$expectedLeaders/+1 actual:$actual", expectedLeaders == actual || expectedLeaders + 1 == actual)
    }
  }
}