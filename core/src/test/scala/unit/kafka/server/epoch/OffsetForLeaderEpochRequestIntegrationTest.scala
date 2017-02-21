/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package unit.kafka.server.epoch

import kafka.server.BaseRequestTest
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.requests.OffsetForLeaderEpochResponse._
import org.apache.kafka.common.requests._
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.JavaConverters._

class OffsetForLeaderEpochRequestIntegrationTest extends BaseRequestTest {
  override def numBrokers: Int = 1

  val topic = "test-topic"
  var consumer: KafkaConsumer[Array[Byte], Array[Byte]] = null
  var producer: KafkaProducer[Array[Byte], Array[Byte]] = null

  @Before
  override def setUp() {
    super.setUp()
    producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers), maxBlockMs = 3000, acks = 1)
    consumer = TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers), groupId = "mygroup", securityProtocol = SecurityProtocol.PLAINTEXT)
    TestUtils.createTopic(zkUtils, topic, 1, 1, this.servers)
  }

  @After
  override def tearDown() = {
    producer.close()
    consumer.close()
    super.tearDown()
  }

  //TODO all the extra bits like authentication etc.
  @Test
  def shouldGetAResponse(): Unit = {

    //Given a topic with 100 messages on a single partition topic
    TestUtils.produceMessages(servers, topic, 100)
    val epochsRequested = Map(topic -> List(new Epoch(0, 0)).asJava).asJava
    val request = new OffsetForLeaderEpochRequest.Builder(epochsRequested).build()

    //When we request offsets for leader epoch 0
    val response = parse(send(request, ApiKeys.OFFSET_FOR_LEADER_EPOCH), 0)

    //We expect it to come back with leader epoch 0, but offset 100
    val expected = Map(topic -> List(new EpochEndOffset(0, 0, 100)).asJava).asJava
    assertTrue(response.responses.containsKey(topic))
    assertEquals(expected, response.responses)
  }

}
