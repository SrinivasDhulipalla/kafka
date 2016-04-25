package kafka.poc

import kafka.common.TopicAndPartition
import kafka.poc.Helper._
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class MovesOptimisedRebalancePolicyTest {

  /**
    * Step 1: Ensure partitions are fully replicated
    */
  @Test
  def shouldFullyReplicateUnderreplicatedPartitions(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = (100 to 104).map(bk(_, "rack1"))
    val underreplicated = Map(p(0) -> List(100, 101, 102))
    val topics = Map("my-topic" -> 4)

    //When
    val reassigned = policy.rebalancePartitions(brokers, underreplicated, topics)

    //Then there should be four values. They should be on different Brokers
    assertEquals(4, reassigned.values.last.size)
    assertEquals(4, reassigned.values.last.distinct.size)
  }

  @Test
  def shouldPickLeastLoadedBrokerWhenReReplicatingUnderreplicatedPartitions(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given broker 102 is least loaded and p[4] is under-replicated
    val brokers = (100 to 103).map(bk(_, "rack1"))
    val underreplicated = Map(
      p(0) -> List(100, 101, 102), //102 has two replicas, 103 has 3, 101 has 4, 100 has 5
      p(1) -> List(100, 102, 103),
      p(2) -> List(100, 101, 103),
      p(3) -> List(100, 101, 103),
      p(4) -> List(100, 101))
    val topics = Map("my-topic" -> 3)

    //When
    val reassigned = policy.rebalancePartitions(brokers, underreplicated, topics)

    //then p[4] should have a new replica on broker 102 (the least loaded)
    assertEquals(List(101, 100, 102), reassigned.get(p(4)).get)
  }

  @Test
  def shouldConsiderRackConstraintWhenPickingLeastLoadedBrokerWhenReReplicatingUnderreplicatedPartitions(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack2"), bk(103, "rack1"))
    val underreplicated = Map(p(0) -> List(100))
    val topics = Map("my-topic" -> 2)

    //When we create a new replica for the under-replicated partition
    val reassigned = policy.rebalancePartitions(brokers, underreplicated, topics)

    //Then it should be created on the broker on a different rack (102)
    assertEquals(List(100, 102), reassigned.get(p(0)).get)
  }

  @Test
  def shouldCreateMultipleReplicasPerPartitionIfNecessary(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given two partitions. One under-replicated by 2 replicas. 2 empty brokers 103/104
    val brokers = (100 to 104).map(bk(_, "rack1"))
    val underreplicated = Map(
      p(0) -> List(100, 101, 102),
      p(1) -> List(100))
    val topics = Map("my-topic" -> 3)

    //When
    val reassigned = policy.rebalancePartitions(brokers, underreplicated, topics)

    //Then p1 should have two new replicas on the two empty brokers, 103, 104
    assertEquals(List(100, 103, 104), reassigned.get(p(1)).get)
  }

  @Test
  def shouldNotReReplicateIfNoBrokerAvailableWithoutExistingReplica(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given two partitions. One under-replicated by 2 replicas. 2 empty brokers 103/104
    val brokers = (100 to 102).map(bk(_, "rack1"))
    val underreplicated = Map(p(0) -> List(100, 101, 102))
    val topics = Map("my-topic" -> 3)

    //When
    val reassigned = policy.rebalancePartitions(brokers, underreplicated, topics)

    //Then nothing should have changed
    assertEquals((100 to 102), reassigned.get(p(0)).get.sorted)
  }


  /**
    * Step 2.1: Optimise for replica fairness across racks
    */
  @Test
  def shouldOptimiseForEvenReplicaPlacementAcrossRacksSimple(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack2"))
    val partitions = Map(
      p(0) -> List(100),
      p(1) -> List(100))
    val topics = Map("my-topic" -> 1)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should be one per rack
    assertEquals(Map(p(0) -> List(100), p(1) -> List(101)), reassigned)
  }

  @Test
  def shouldOptimiseForEvenReplicaPlacementAcrossRacks(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given replicas all live on rack1
    val brokers = (100 to 101).map(bk(_, "rack1")) ++ (102 to 103).map(bk(_, "rack2"))
    val partitions = Map(
      p(0) -> List(100),
      p(1) -> List(101),
      p(2) -> List(100),
      p(3) -> List(101)
    )
    val topics = Map("my-topic" -> 1)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should end evenly spread
    assertEquals((100 to 103).toSeq, reassigned.values.flatten.toSeq.sorted)
  }

  @Test
  def shouldOptimiseForEvenReplicaPlacementAcrossManyRacks(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given replicas all live on rack1
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack2"), bk(103, "rack2"), bk(104, "rack3"), bk(105, "rack3"))
    val partitions = Map(
      p(0) -> List(100),
      p(1) -> List(101),
      p(2) -> List(100),
      p(3) -> List(101),
      p(4) -> List(100),
      p(5) -> List(101)

    )
    val topics = Map("my-topic" -> 1)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should end evenly spread one replica per broker and hence two per rack
    assertEquals((100 to 105).toSeq, reassigned.values.flatten.toSeq.sorted)
  }

  @Test
  def shouldOptimiseForEvenReplicaPlacementWhereThereAreMoveAboveParReplicasThanBelowParOpenings(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given replicas all live on rack1
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack2"))
    val partitions = Map(
      p(0) -> List(100),
      p(1) -> List(100),
      p(2) -> List(101),
      p(3) -> List(101)
    )
    val topics = Map("my-topic" -> 1)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should end evenly spread one replica per broker and two replicas per rack
    assertEquals(Map(
      p(0) -> List(100),
      p(1) -> List(100),
      p(2) -> List(101),
      p(3) -> List(102)), reassigned)
  }

  /**
    * Step 2.2: Optimise for leader fairness across racks
    */
  @Test
  def shouldOptimiseForLeaderFairnessAcrossRacks(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack2"))
    val partitions = Map(
      p(0) -> List(100, 101),
      p(1) -> List(100, 101))
    val topics = Map("my-topic" -> 2)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should be one per rack
    assertEquals(Map(p(0) -> List(100, 101), p(1) -> List(101, 100)), reassigned)
  }

  @Test
  def shouldOptimiseForLeaderFairnessAcrossThreeRacks(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack2"), bk(102, "rack3"))
    val partitions = Map(
      p(0) -> List(100, 101),
      p(1) -> List(100, 102),
      p(2) -> List(101, 102))
    val topics = Map("my-topic" -> 2)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should be one per rack
    assertEquals(Map(p(0) -> List(100, 101), p(1) -> List(102, 100), p(2) -> List(101, 102)), reassigned)
  }

  @Test
  def shouldNotOptimiseIfAlreadyFair(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack2"))
    val partitions = Map(
      p(0) -> List(100, 101),
      p(1) -> List(101, 100))
    val topics = Map("my-topic" -> 2)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should be one per rack
    assertEquals(Map(p(0) -> List(100, 101), p(1) -> List(101, 100)), reassigned)
  }

  /**
    * Step 3.1: Optimise for replica fairness across brokers
    */
  @Test
  def shouldOptimiseForEvenReplicaPlacementAcrossBrokersSimple(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"))
    val partitions = Map(
      p(0) -> List(100),
      p(1) -> List(100))
    val topics = Map("my-topic" -> 1)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should be one per rack
    assertEquals(Map(p(0) -> List(100), p(1) -> List(101)), reassigned)
  }

  @Test
  def shouldOptimiseForEvenReplicaPlacementAcrossBrokers(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack1"), bk(103, "rack1"))
    val partitions = Map(
      p(0) -> List(100),
      p(1) -> List(100),
      p(2) -> List(100),
      p(3) -> List(100)
    )
    val topics = Map("my-topic" -> 1)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should be one per broker
    assertEquals(sort(Map(p(0) -> List(103), p(1) -> List(101), p(2) -> List(100), p(3) -> List(102))), sort(reassigned.toMap))
  }


  @Test
  def shouldMoveReplicasToLeastLoadedBroker(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack1"), bk(103, "rack1"))
    //100 -> 2 replicas, 101-> 3 replicas, 102 -> 2 replicas, 103 -> 1 replica
    val partitions = Map(
      p(0) -> List(100, 101),
      p(1) -> List(100, 102),
      p(2) -> List(102, 101),
      p(3) -> List(103, 101)
    )
    val topics = Map("my-topic" -> 2)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should have moved one replica from 101 -> 103
    assertEquals(sort(Map(
      p(0) -> Seq(100, 103),
      p(1) -> Seq(100, 102),
      p(2) -> Seq(102, 101),
      p(3) -> Seq(103, 101))), sort(reassigned.toMap))
  }

  @Test
  def shouldNotMoveReplicaIfBreaksRackConstraint(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack2"), bk(102, "rack2"))
    val partitions = Map(
      p(0) -> List(100, 101),
      p(1) -> List(100, 101),
      p(2) -> List(100, 101),
      p(3) -> List(100, 101),
      p(4) -> List(100, 101),
      p(5) -> List(100, 101)
    )
    val topics = Map("my-topic" -> 2)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)
    println(reassigned)
    //All replicas on 100 should remain there (i.e. on rack 1)
    assertEquals(6, reassigned.values.flatten.filter(_ == 100).size)
  }


  /**
    * Step 3.2: Optimise for leader fairness across brokers
    */
  @Test
  def shouldOptimiseForLeaderFairnessAcrossBrokersSimple(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"))
    val partitions = Map(
      p(0) -> List(100, 101),
      p(1) -> List(100, 101))
    val topics = Map("my-topic" -> 2)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should be one per broker
    val leaders = reassigned.values.map(_ (0))
    assertEquals(2, leaders.toSeq.distinct.size)
  }

  @Test
  def shouldOptimiseForLeaderFairnessAcrossBrokers(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack1"))
    val partitions = Map(
      p(0) -> List(100, 101, 102),
      p(1) -> List(100, 101, 102),
      p(3) -> List(100, 101, 102))
    val topics = Map("my-topic" -> 3)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should be one per broker
    val leaders = reassigned.values.map(_ (0))
    assertEquals(3, leaders.toSeq.distinct.size)
  }

  /**
    * Tests for multiple topics
    */

  @Test
  def shouldBalanceLeadersOverMultipleTopicsSingleRack(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack1"))
    val partitions = Map(
      p(0, "t1") -> List(100, 101, 102),
      p(0, "t2") -> List(100, 101, 102),
      p(0, "t3") -> List(100, 101, 102))
    val topics = Map("t1" -> 3, "t2" -> 3, "t3" -> 3)

    assertEquals(1, partitions.values.map(_ (0)).toSeq.distinct.size)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should be one leader per broker
    val leaders = reassigned.values.map(_ (0))
    assertEquals(3, leaders.toSeq.distinct.size)
  }

  @Test //This test seems to lose a replica because it has a partition that brakes the partition cosntraint
  def shouldBalanceLeadersOverMultipleTopicsAndMultipleRack(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack2"), bk(103, "rack2"))
    val partitions = Map(
      p(0, "t1") -> List(100, 101, 102),
      p(0, "t2") -> List(100, 101, 102),
      p(0, "t3") -> List(100, 101, 102),
      p(0, "t4") -> List(100, 101, 102))
    val topics = Map("t1" -> 3, "t2" -> 3, "t3" -> 3, "t4" -> 3)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should be one leader per broker
    val leaders = reassigned.values.map(_ (0))
    assertEquals(4, leaders.toSeq.distinct.size)
  }

  @Test
  def shouldBalanceReplicasOverMultipleTopicsSingleRack(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack1"))
    val partitions = Map(
      p(0, "t1") -> List(100, 101),
      p(0, "t2") -> List(100, 101),
      p(0, "t3") -> List(100, 101))
    val topics = Map("t1" -> 2, "t2" -> 2, "t3" -> 2)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then
    val numberReplicasOn102 = reassigned.values.flatten.filter(_ == 102).size
    assertEquals(2, numberReplicasOn102)
  }


  /**
    * Test Move & Leader Functions
    */

  @Test
  def shouldMakeLeader(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"))
    val partitions = mutable.Map(
      p(0) -> Seq(100, 101))

    //When
    policy.makeLeader(p(0), 101, partitions)

    //Then
    assertEquals(Seq(101, 100), partitions.get(p(0)).get)
  }


  @Test
  def shouldMakeLeaderDoNothingIfMakingExistingLeaderTheLeader(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"))
    val partitions = mutable.Map(
      p(0) -> Seq(100, 101))

    //When
    policy.makeLeader(p(0), 100, partitions)

    //Then
    assertEquals(Seq(100, 101), partitions.get(p(0)).get)
  }



  def sort(x: Map[TopicAndPartition, Seq[Int]]) = {
    x.toSeq.sortBy(_._1.partition)
  }
}
