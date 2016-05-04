package kafka.poc

import kafka.common.TopicAndPartition


class Replica(val topic: String, val partitionId: Int, val broker: Int) {
  def partition(): TopicAndPartition = {
    new TopicAndPartition(topic, partitionId)
  }

  override def toString = s"Replica[$topic:$partitionId:$broker]"

  def canEqual(other: Any): Boolean = other.isInstanceOf[Replica]

  override def equals(other: Any): Boolean = other match {
    case that: Replica =>
      (that canEqual this) &&
        topic == that.topic &&
        partitionId == that.partitionId &&
        broker == that.broker
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(topic, partitionId, broker)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
