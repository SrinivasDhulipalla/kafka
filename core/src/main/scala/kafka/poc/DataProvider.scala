package kafka.poc

import kafka.admin.BrokerMetadata
import kafka.common.TopicAndPartition

import scala.collection.{Map, Iterable, Seq}

/**
  * Created by benji on 06/05/2016.
  */
class DataProvider (brokers: Seq[BrokerMetadata], partitions: Map[TopicAndPartition, Seq[Int]]) {


}
