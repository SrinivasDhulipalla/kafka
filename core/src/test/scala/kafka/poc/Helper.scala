package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

object Helper {

  val topic: String = "my-topic"
  def p(i: Int) = {
    new TopicAndPartition(topic, i)
  }

  def p(i: Int, topic: String) = {
    new TopicAndPartition(topic, i)
  }

  def bk(id: Int, rack: String) = {
    new BrokerMetadata(id, Option(rack))
  }

  def r(r: Int)={
    Map("my-topic" -> r)
  }

}
