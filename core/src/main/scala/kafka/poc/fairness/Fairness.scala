package kafka.poc.fairness

import kafka.admin.BrokerMetadata

import scala.collection.Seq

/**
  * NB: The definition of fairness must incorporate the use of ceil/floor when dealing with fractional values.
  * Below Par (i.e. below the fair value) is always defined with ceil(). AbovePar is always defined with floor().
  * Thus, where fairness is a fractional value, there will be an overlap between abovePar and belowPar.
  * This is required as the algorithm is not symmetric (it starts at with abovePar brokers then searches
  * belowPar brokers).
  *
  * Consider the an arrangement where the replica counts on each broker are (1, 1, 2, 3, 3). There are 11
  * replicas over 5 brokers so fairness = 2.2. Thus ceil(fairvalue) -> 3 and would not achieve fairness.
  * Rerunning in the opposite direction (belowPar->abovePar) also does not help  as there are no above
  * par options to move to. Hence we choose to apply ceil/floor to the abovePar/belowPar to create an overlapping
  * intersection.
  */
trait Fairness {
  def aboveParRacks: Seq[String]

  def belowParRacks: Seq[String]

  def aboveParBrokers: Seq[BrokerMetadata]

  def belowParBrokers: Seq[BrokerMetadata]
}

