package kafka.poc

import kafka.poc.Helper._
import kafka.poc.view.BrokerFairView
import kafka.poc.view.{BrokerFairView, RackFairView}
import org.junit.Assert._
import org.junit.Test

class FairnessTest {

  @Test
  def shouldCalculateReplicaFairnessForRacks(): Unit = {
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack2"), bk(103, "rack2"))

    //1 replica, 2 racks. rack1 is Below Par, rack2 is onPar.
    var view = new RackFairView(brokers, Map(p(0) -> List(101)))
    assertEquals(1, view.replicaFairness.rackFairValue)
    assertEquals(Seq(), view.replicaFairness.aboveParRacks)
    assertEquals(Seq("rack2"), view.replicaFairness.belowParRacks)

    //2 replicas, 2 racks, both replicas on one rack
    view = new RackFairView(brokers, Map(p(0) -> List(103, 102)))
    assertEquals(1, view.replicaFairness.rackFairValue)
    assertEquals(Seq("rack2"), view.replicaFairness.aboveParRacks())
    assertEquals(Seq("rack1"), view.replicaFairness.belowParRacks())

    //2 replicas, 2 racks, replicas on different racks
    view = new RackFairView(brokers, Map(p(0) -> List(100), p(1) -> List(102)))
    assertEquals(1, view.replicaFairness.rackFairValue)
    assertEquals(Seq(), view.replicaFairness.aboveParRacks())
    assertEquals(Seq(), view.replicaFairness.belowParRacks())

    //3 replicas, 2 racks, replicas spread 2/1
    view = new RackFairView(brokers, Map(p(0) -> List(100, 101, 103)))
    assertEquals(2, view.replicaFairness.rackFairValue)
    assertEquals(Seq(), view.replicaFairness.aboveParRacks())
    assertEquals(Seq("rack2"), view.replicaFairness.belowParRacks())

    //3 replicas, 2 racks, replicas spread 3/0
    view = new RackFairView(brokers, Map(p(0) -> List(100, 101), p(1) -> List(100)))
    assertEquals(2, view.replicaFairness.rackFairValue)
    assertEquals(Seq("rack1"), view.replicaFairness.aboveParRacks())
    assertEquals(Seq("rack2"), view.replicaFairness.belowParRacks())

    //4 replicas, 2 racks, spread 2/2
    view = new RackFairView(brokers, Map(p(0) -> List(103, 102, 101, 100)))
    assertEquals(2, view.replicaFairness.rackFairValue)
    assertEquals(Seq(), view.replicaFairness.aboveParRacks())
    assertEquals(Seq(), view.replicaFairness.belowParRacks())

    //4 replicas, 2 racks, spread 3/1
    view = new RackFairView(brokers, Map(p(0) -> List(100), p(1) -> List(100), p(2) -> List(100), p(3) -> List(103)))
    assertEquals(2, view.replicaFairness.rackFairValue)
    assertEquals(Seq("rack1"), view.replicaFairness.aboveParRacks())
    assertEquals(Seq("rack2"), view.replicaFairness.belowParRacks())

    //4 replicas, 2 racks, spread 4/0
    view = new RackFairView(brokers, Map(p(0) -> List(100), p(1) -> List(100), p(2) -> List(100), p(3) -> List(100)))
    assertEquals(2, view.replicaFairness.rackFairValue)
    assertEquals(Seq("rack1"), view.replicaFairness.aboveParRacks())
    assertEquals(Seq("rack2"), view.replicaFairness.belowParRacks())

  }

  @Test
  def shouldCalculateReplicaFairnessForBrokers(): Unit = {
    val bk0 = bk(100, "rack1")
    val bk1 = bk(101, "rack1")

    //1 replica, 2 brokers. bk1 is Below Par, bk2 is onPar.
    var view = new BrokerFairView(List(bk0, bk1), Map(p(0) -> List(101)), "rack1")
    assertEquals(1, view.replicaFairness.brokerFairValue)
    assertEquals(Seq(), view.replicaFairness.aboveParBrokers)
    assertEquals(Seq(bk0), view.replicaFairness.belowParBrokers)

    //2 replicas, 2 brokers, both replicas on one broker
    view = new BrokerFairView(List(bk0, bk1), Map(p(0) -> List(100), p(1) -> List(100)), "rack1")
    assertEquals(1, view.replicaFairness.brokerFairValue)
    assertEquals(Seq(bk0), view.replicaFairness.aboveParBrokers())
    assertEquals(Seq(bk1), view.replicaFairness.belowParBrokers())

    //2 replicas, 2 brokers, replicas on each
    view = new BrokerFairView(List(bk0, bk1), Map(p(0) -> List(100), p(1) -> List(101)), "rack1")
    assertEquals(1, view.replicaFairness.brokerFairValue)
    assertEquals(Seq(), view.replicaFairness.aboveParBrokers())
    assertEquals(Seq(), view.replicaFairness.belowParBrokers())

    //3 replicas, 2 brokers, replicas spread 2/1
    view = new BrokerFairView(List(bk0, bk1), Map(p(0) -> List(100, 101), p(1) -> List(100)), "rack1")
    assertEquals(2, view.replicaFairness.brokerFairValue)
    assertEquals(Seq(), view.replicaFairness.aboveParBrokers())
    assertEquals(Seq(bk1), view.replicaFairness.belowParBrokers())

    //3 replicas, 2 brokers, replicas spread 3/0
    view = new BrokerFairView(List(bk0, bk1), Map(p(0) -> List(100), p(1) -> List(100), p(2) -> List(100)), "rack1")
    assertEquals(2, view.replicaFairness.brokerFairValue)
    assertEquals(Seq(bk0), view.replicaFairness.aboveParBrokers())
    assertEquals(Seq(bk1), view.replicaFairness.belowParBrokers())

    //4 replicas, 2 brokers, spread 2/2
    view = new BrokerFairView(List(bk0, bk1), Map(p(0) -> List(100, 101), p(1) -> List(100, 101)), "rack1")
    assertEquals(2, view.replicaFairness.brokerFairValue)
    assertEquals(Seq(), view.replicaFairness.aboveParBrokers())
    assertEquals(Seq(), view.replicaFairness.belowParBrokers())

    //4 replicas, 2 brokers, spread 3/1
    view = new BrokerFairView(List(bk0, bk1), Map(p(0) -> List(100), p(1) -> List(100), p(2) -> List(100), p(3) -> List(101)), "rack1")
    assertEquals(2, view.replicaFairness.brokerFairValue)
    assertEquals(Seq(bk0), view.replicaFairness.aboveParBrokers())
    assertEquals(Seq(bk1), view.replicaFairness.belowParBrokers())

    //4 replicas, 2 brokers, spread 4/0
    view = new BrokerFairView(List(bk0, bk1), Map(p(0) -> List(100), p(1) -> List(100), p(2) -> List(100), p(3) -> List(100)), "rack1")
    assertEquals(2, view.replicaFairness.brokerFairValue)
    assertEquals(Seq(bk0), view.replicaFairness.aboveParBrokers())
    assertEquals(Seq(bk1), view.replicaFairness.belowParBrokers())

  }

  @Test
  def shouldCalculateLeaderFairnessForRacks(): Unit = {
    val brokers = List(bk(100, "rack1"), bk(101, "rack1"), bk(102, "rack2"), bk(103, "rack2"))

    //1 leader, 2 racks. rack1 is Below Par, rack2 is onPar.
    var view = new RackFairView(brokers, Map(p(0) -> List(101)))
    assertEquals(1, view.leaderFairness.rackFairValue)
    assertEquals(Seq(), view.replicaFairness.aboveParRacks)
    assertEquals(Seq("rack2"), view.replicaFairness.belowParRacks)

    //2 leaders, 2 racks, both leaders on one rack
    view = new RackFairView(brokers, Map(p(0) -> List(102),p(1) -> List(102)))
    assertEquals(1, view.leaderFairness.rackFairValue)
    assertEquals(Seq("rack2"), view.replicaFairness.aboveParRacks())
    assertEquals(Seq("rack1"), view.replicaFairness.belowParRacks())

    //2 leaders, 2 racks, leaders on different racks
    view = new RackFairView(brokers, Map(p(0) -> List(100), p(1) -> List(102)))
    assertEquals(1, view.leaderFairness.rackFairValue)
    assertEquals(Seq(), view.replicaFairness.aboveParRacks())
    assertEquals(Seq(), view.replicaFairness.belowParRacks())

    //3 leaders, 2 racks, replicas spread 2/1
    view = new RackFairView(brokers, Map(p(0) -> List(100), p(1) -> List(100), p(2) -> List(102)))
    assertEquals(2, view.leaderFairness.rackFairValue)
    assertEquals(Seq(), view.replicaFairness.aboveParRacks())
    assertEquals(Seq("rack2"), view.replicaFairness.belowParRacks())

    //3 leader, 2 racks, replicas spread 3/0
    view = new RackFairView(brokers, Map(p(0) -> List(100), p(1) -> List(100), p(2) -> List(100)))
    assertEquals(2, view.leaderFairness.rackFairValue)
    assertEquals(Seq("rack1"), view.replicaFairness.aboveParRacks())
    assertEquals(Seq("rack2"), view.replicaFairness.belowParRacks())

    //4 leader, 2 racks, spread 2/2
    view = new RackFairView(brokers, Map(p(0) -> List(100), p(1) -> List(100), p(2) -> List(103), p(3) -> List(103)))
    assertEquals(2, view.leaderFairness.rackFairValue)
    assertEquals(Seq(), view.replicaFairness.aboveParRacks())
    assertEquals(Seq(), view.replicaFairness.belowParRacks())

    //4 leader, 2 racks, spread 3/1
    view = new RackFairView(brokers, Map(p(0) -> List(100), p(1) -> List(100), p(2) -> List(100), p(3) -> List(103)))
    assertEquals(2, view.leaderFairness.rackFairValue)
    assertEquals(Seq("rack1"), view.replicaFairness.aboveParRacks())
    assertEquals(Seq("rack2"), view.replicaFairness.belowParRacks())

    //4 leader, 2 racks, spread 4/0
    view = new RackFairView(brokers, Map(p(0) -> List(100), p(1) -> List(100), p(2) -> List(100), p(3) -> List(100)))
    assertEquals(2, view.leaderFairness.rackFairValue)
    assertEquals(Seq("rack1"), view.replicaFairness.aboveParRacks())
    assertEquals(Seq("rack2"), view.replicaFairness.belowParRacks())

  }


  @Test
  def shouldCalculateLeaderFairnessForBrokers(): Unit = {
    val bk0 = bk(100, "rack1")
    val bk1 = bk(101, "rack1")

    //1 leader, 2 brokers. bk1 is Below Par, bk2 is onPar.
    var view = new BrokerFairView(List(bk0, bk1), Map(p(0) -> List(101)), "rack1")
    assertEquals(1, view.leaderFairness.brokerFairValue)
    assertEquals(Seq(), view.leaderFairness.aboveParBrokers)
    assertEquals(Seq(bk0), view.leaderFairness.belowParBrokers)

    //2 leaders, 2 brokers, both replicas on one broker
    view = new BrokerFairView(List(bk0, bk1), Map(p(0) -> List(100), p(1) -> List(100)), "rack1")
    assertEquals(1, view.leaderFairness.brokerFairValue)
    assertEquals(Seq(bk0), view.leaderFairness.aboveParBrokers())
    assertEquals(Seq(bk1), view.leaderFairness.belowParBrokers())

    //2 leaders, 2 brokers, replicas on each
    view = new BrokerFairView(List(bk0, bk1), Map(p(0) -> List(100), p(1) -> List(101)), "rack1")
    assertEquals(1, view.leaderFairness.brokerFairValue)
    assertEquals(Seq(), view.leaderFairness.aboveParBrokers())
    assertEquals(Seq(), view.leaderFairness.belowParBrokers())

    //3 leaders, 2 brokers, replicas spread 2/1
    view = new BrokerFairView(List(bk0, bk1), Map(p(0) -> List(100), p(1) -> List(100), p(2) -> List(101)), "rack1")
    assertEquals(2, view.leaderFairness.brokerFairValue)
    assertEquals(Seq(), view.leaderFairness.aboveParBrokers())
    assertEquals(Seq(bk1), view.leaderFairness.belowParBrokers())

    //3 leaders, 2 brokers, replicas spread 3/0
    view = new BrokerFairView(List(bk0, bk1), Map(p(0) -> List(100), p(1) -> List(100), p(2) -> List(100)), "rack1")
    assertEquals(2, view.leaderFairness.brokerFairValue)
    assertEquals(Seq(bk0), view.leaderFairness.aboveParBrokers())
    assertEquals(Seq(bk1), view.leaderFairness.belowParBrokers())

    //4 leaders, 2 brokers, spread 2/2
    view = new BrokerFairView(List(bk0, bk1), Map(p(0) -> List(100), p(1) -> List(100), p(2) -> List(101), p(3) -> List(101)), "rack1")
    assertEquals(2, view.leaderFairness.brokerFairValue)
    assertEquals(Seq(), view.leaderFairness.aboveParBrokers())
    assertEquals(Seq(), view.leaderFairness.belowParBrokers())

    //4 leaders, 2 brokers, spread 3/1
    view = new BrokerFairView(List(bk0, bk1), Map(p(0) -> List(100), p(1) -> List(100), p(2) -> List(100), p(3) -> List(101)), "rack1")
    assertEquals(2, view.leaderFairness.brokerFairValue)
    assertEquals(Seq(bk0), view.leaderFairness.aboveParBrokers())
    assertEquals(Seq(bk1), view.leaderFairness.belowParBrokers())

    //4 leaders, 2 brokers, spread 4/0
    view = new BrokerFairView(List(bk0, bk1), Map(p(0) -> List(100), p(1) -> List(100), p(2) -> List(100), p(3) -> List(100)), "rack1")
    assertEquals(2, view.leaderFairness.brokerFairValue)
    assertEquals(Seq(bk0), view.leaderFairness.aboveParBrokers())
    assertEquals(Seq(bk1), view.leaderFairness.belowParBrokers())

  }


}
