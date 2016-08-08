/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.server

import kafka.cluster.BrokerEndPoint
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time

class ReplicaFetcherManager(brokerConfig: KafkaConfig, replicaMgr: ReplicaManager, metrics: Metrics, time: Time, threadNamePrefix: Option[String] = None, quotaManager: ClientQuotaManager, isThrottled: Boolean)
  extends AbstractFetcherManager(
    if (isThrottled) "Throttled" else "" + "ReplicaFetcherManager on broker " + brokerConfig.brokerId,
    if (isThrottled) "ThrottledReplica" else "Replica", brokerConfig.numReplicaFetchers) {

§
  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread = {
    val threadName = threadNamePrefix match {
      case None =>
        "%sFetcherThread-%d-%d".format(clientId, fetcherId, sourceBroker.id)
      case Some(p) =>
        "%s:%sFetcherThread-%d-%d".format(p, clientId, fetcherId, sourceBroker.id)
    }
    val id = if (isThrottled) fetcherId + 1000 else fetcherId //TODO we should have a better mechanism than this for managing fetcherIds
    new ReplicaFetcherThread(threadName, id, sourceBroker, brokerConfig,
      replicaMgr, metrics, time, quotaManager, isThrottled)
  }

  def shutdown() {
    info("shutting down")
    closeAllFetchers()
    info("shutdown completed")
  }
}
