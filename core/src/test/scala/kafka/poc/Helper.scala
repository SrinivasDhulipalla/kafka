package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

object Helper {
  def p(i: Int) = {
    new TopicAndPartition("my-topic", i)
  }

  def bk(id: Int, rack: String) = {
    new BrokerMetadata(id, Option(rack))
  }

}
