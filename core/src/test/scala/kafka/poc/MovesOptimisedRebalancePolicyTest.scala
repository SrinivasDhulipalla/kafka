package kafka.poc

import java.util

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition
import org.junit.Test
import org.junit.Assert._

import scala.collection.Seq
import scala.collection.immutable.IndexedSeq

class MovesOptimisedRebalancePolicyTest {

  val temp = Map(
    p(0) -> List(100, 101, 102),
    p(1) -> List(100, 101, 102),
    p(2) -> List(100, 101, 102),
    p(3) -> List(100, 101, 102),
    p(4) -> List(100, 101, 102),
    p(5) -> List(100, 101, 102),
    p(6) -> List(100, 101, 102),
    p(7) -> List(100, 101, 102),
    p(8) -> List(100, 101, 102),
    p(9) -> List(100, 101, 102))

  def p(i: Int) = {
    new TopicAndPartition("my-topic", i)
  }

  def bk(id: Int, rack: String) = {
    new BrokerMetadata(id, Option(rack))
  }


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

  //Start here tomorrow. Looks like something is wrong with ordering or least loaded

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
    assertEquals(List(100, 101, 102), reassigned.get(p(4)).get)
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
  def todo(): Unit = {
    val policy = new MovesOptimisedRebalancePolicy()

    //Given
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack2"), bk(103, "rack2"))
    val assignment = Map(p(0) -> List(100, 101, 100, 101))
    val topics = Map("my-topic" -> 4)

    //When
    val reassigned = policy.rebalancePartitions(brokers, assignment, topics)

    assertEquals(reassigned.values.iterator.next(), List(100, 101, 102, 103))

  }
}
