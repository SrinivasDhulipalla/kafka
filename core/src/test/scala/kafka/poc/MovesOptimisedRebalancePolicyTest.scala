package kafka.poc

import kafka.poc.Helper._
import org.junit.Assert._
import org.junit.Test

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

    //p1 should have two new replicas on the two empty brokers, 103, 104
    assertEquals(List(100, 103, 104), reassigned.get(p(1)).get)
  }

  @Test
  def shouldNotReReplicateIfNoBrokerAvailableWithoutExistingReplica(): Unit = {
    //TODO
  }


  /**
    * Step 2.1: Optimise for replica fairness across racks
    */
  @Test
  def shouldOptimiseForEvenReplicaPlacementAcrossRacks(): Unit = {
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
  def shouldOptimiseForEvenReplicaPlacementAcrossRacks2(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = (100 to 101).map(bk(_, "rack1"))

    val partitions = Map(
      p(0) -> List(100),
      p(1) -> List(100),
      p(2) -> List(100),
      p(3) -> List(100)
    )
    val topics = Map("my-topic" -> 1)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should be one per rack
    assertEquals(Map(p(0) -> List(100), p(1) -> List(101),p(2) -> List(100), p(3) -> List(101)), reassigned)
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
    val topics = Map("my-topic" -> 1)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should be one per rack
    assertEquals(Map(p(0) -> List(100, 101), p(1) -> List(101, 100)), reassigned)
  }

  /**
    * Step 3.1: Optimise for replica fairness across brokers
    */
  @Test
  def shouldOptimiseForEvenReplicaPlacementAcrossBrokers(): Unit = {
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

  /**
    * Step 3.2: Optimise for leader fairness across brokers
    */
  @Test
  def shouldOptimiseForLeaderFairnessAcrossBrokers(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"))
    val partitions = Map(
      p(0) -> List(100, 101),
      p(1) -> List(100, 101))
    val topics = Map("my-topic" -> 1)

    //When
    val reassigned = policy.rebalancePartitions(brokers, partitions, topics)

    //Then should be one per rack
    assertEquals(Map(p(0) -> List(100, 101), p(1) -> List(101, 100)), reassigned)
  }
}
