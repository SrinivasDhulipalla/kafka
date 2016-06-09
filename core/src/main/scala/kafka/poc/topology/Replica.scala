package kafka.poc.topology

import kafka.common.TopicAndPartition


case class Replica(val topic: String, val partitionId: Int, val broker: Int) {
  def partition(): TopicAndPartition = {
    new TopicAndPartition(topic, partitionId)
  }

  override def toString = s"r[$topic:$partitionId:$broker]"

}
