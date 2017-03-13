/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package unit.kafka.server.epoch.util

import java.net.SocketTimeoutException

import kafka.server.{BlockingSend, KafkaConfig}
import org.apache.kafka.clients.{ClientResponse, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.AbstractRequest.Builder
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.{SystemTime, Time}

import scala.collection.JavaConverters._

class TestSender(destination: Node, brokerConfig: KafkaConfig, metrics: Metrics, time: Time) extends BlockingSend{


  private val networkClient = {
    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      brokerConfig.interBrokerSecurityProtocol,
      JaasContext.Type.SERVER,
      brokerConfig,
      brokerConfig.interBrokerListenerName,
      brokerConfig.saslMechanismInterBrokerProtocol,
      brokerConfig.saslInterBrokerHandshakeRequestEnable
    )
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      brokerConfig.connectionsMaxIdleMs,
      metrics,
      time,
      "test-sender",
      Map("broker-id" -> brokerConfig.brokerId.toString).asJava,
      false,
      channelBuilder
    )
    new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      "TestSender",
      1,
      0,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      brokerConfig.replicaSocketReceiveBufferBytes,
      brokerConfig.requestTimeoutMs,
      time,
      false
    )
  }


  override def sendRequest(requestBuilder: Builder[_ <: AbstractRequest]): ClientResponse = {
    val timeout = 30 * 1000
    val time = new SystemTime
    import kafka.utils.NetworkClientBlockingOps._
    try {
      if (!networkClient.blockingReady(destination, timeout)(time))
        throw new SocketTimeoutException(s"Failed to connect within $timeout ms")
      else {
        val clientRequest = networkClient.newClientRequest(destination.id.toString, requestBuilder,
          time.milliseconds(), true)
        networkClient.blockingSendAndReceive(clientRequest)(time)
      }
    }
    catch {
      case e: Throwable =>
        networkClient.close(destination.id.toString)
        throw e
    }
  }

  def close(): Unit ={
    networkClient.close()
  }
}

