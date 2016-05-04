package kafka.poc.fairness

import kafka.admin.BrokerMetadata

import scala.collection.Seq

trait Fairness {
  def aboveParRacks: Seq[String]

  def belowParRacks: Seq[String]

  def aboveParBrokers: Seq[BrokerMetadata]

  def belowParBrokers: Seq[BrokerMetadata]
}

