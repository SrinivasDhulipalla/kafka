package kafka.server

import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.ApiKeys

import scala.collection.Map

object QuotaManagerFactory {

  object QuotaType extends Enumeration{
    val ClientFetch, ClientProduce, LeaderReplication, FollowerReplication = Value
  }
  /*
   * Returns a Map of all quota managers configured. The request Api key is the key for the Map
   */
  def instantiate(cfg: KafkaConfig, metrics: Metrics): Map[Short, ClientQuotaManager] = {
    val producerQuotaManagerCfg = ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.producerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )

    val consumerQuotaManagerCfg = ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.consumerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )

    val quotaManagers = Map[Short, ClientQuotaManager](
      ApiKeys.PRODUCE.id ->
        new ClientQuotaManager(producerQuotaManagerCfg, metrics, ApiKeys.PRODUCE.name, new org.apache.kafka.common.utils.SystemTime),
      ApiKeys.FETCH.id ->
        new ClientQuotaManager(consumerQuotaManagerCfg, metrics, ApiKeys.FETCH.name, new org.apache.kafka.common.utils.SystemTime),
      TempThrottleTypes.leaderThrottleApiKey ->
        new ClientQuotaManager(consumerQuotaManagerCfg, metrics,  TempThrottleTypes.leaderThrottleApiName, new org.apache.kafka.common.utils.SystemTime),
      TempThrottleTypes.followerThrottleApiKey ->
        new ClientQuotaManager(consumerQuotaManagerCfg, metrics,  TempThrottleTypes.followerThrottleApiName, new org.apache.kafka.common.utils.SystemTime)
    )
    quotaManagers
  }
}
