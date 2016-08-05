package kafka.server

import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.ApiKeys

import scala.collection.Map

object QuotaManagerFactory {

  object QuotaType extends Enumeration{
    val Fetch, Produce, LeaderReplication, FollowerReplication = Value
  }
  /*
   * Returns a Map of all quota managers configured. The request Api key is the key for the Map
   */
  def instantiate(cfg: KafkaConfig, metrics: Metrics): Map[QuotaType.Value, ClientQuotaManager] = {
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

    val quotaManagers = Map[QuotaType.Value, ClientQuotaManager](
      QuotaType.Produce ->
        new ClientQuotaManager(producerQuotaManagerCfg, metrics, QuotaType.Produce.toString, new org.apache.kafka.common.utils.SystemTime),
      QuotaType.Fetch ->
        new ClientQuotaManager(consumerQuotaManagerCfg, metrics, QuotaType.Fetch.toString, new org.apache.kafka.common.utils.SystemTime),
      QuotaType.LeaderReplication ->
        new ClientQuotaManager(consumerQuotaManagerCfg, metrics,  QuotaType.LeaderReplication.toString, new org.apache.kafka.common.utils.SystemTime),
      QuotaType.FollowerReplication ->
        new ClientQuotaManager(consumerQuotaManagerCfg, metrics,   QuotaType.FollowerReplication.toString, new org.apache.kafka.common.utils.SystemTime)
    )
    quotaManagers
  }
}
