package kafka.server

import java.net.SocketTimeoutException

import kafka.cluster.BrokerEndPoint
import org.apache.kafka.clients.{ClientResponse, ManualMetadataUpdater, NetworkClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.utils.Time
import scala.collection.JavaConverters._

trait BlockingSend {
  def sendRequest(requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]): ClientResponse

  def close()
}

class ReplicaFetcherBlockingSend(sourceBroker: BrokerEndPoint,
                                 brokerConfig: KafkaConfig,
                                 metrics: Metrics,
                                 time: Time,
                                 fetcherId: Int,
                                 clientId: String) extends BlockingSend {

  private val sourceNode = new Node(sourceBroker.id, sourceBroker.host, sourceBroker.port)
  private val socketTimeout: Int = brokerConfig.replicaSocketTimeoutMs

  private val networkClient = {
    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      brokerConfig.interBrokerSecurityProtocol,
      LoginType.SERVER,
      brokerConfig.values,
      brokerConfig.saslMechanismInterBrokerProtocol,
      brokerConfig.saslInterBrokerHandshakeRequestEnable
    )
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      brokerConfig.connectionsMaxIdleMs,
      metrics,
      time,
      "replica-fetcher",
      Map("broker-id" -> sourceBroker.id.toString, "fetcher-id" -> fetcherId.toString).asJava,
      false,
      channelBuilder
    )
    new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      clientId,
      1,
      0,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      brokerConfig.replicaSocketReceiveBufferBytes,
      brokerConfig.requestTimeoutMs,
      time,
      false
    )
  }

  def sendRequest(requestBuilder: AbstractRequest.Builder[_ <: AbstractRequest]): ClientResponse = {
    import kafka.utils.NetworkClientBlockingOps._
    try {
      if (!networkClient.blockingReady(sourceNode, socketTimeout)(time))
        throw new SocketTimeoutException(s"Failed to connect within $socketTimeout ms")
      else {
        val clientRequest = networkClient.newClientRequest(
          sourceBroker.id.toString,
          requestBuilder,
          time.milliseconds(),
          true)
        networkClient.blockingSendAndReceive(clientRequest)(time)
      }
    }
    catch {
      case e: Throwable =>
        networkClient.close(sourceBroker.id.toString)
        throw e
    }
  }

  def close(): Unit = {
    networkClient.close()
  }
}
