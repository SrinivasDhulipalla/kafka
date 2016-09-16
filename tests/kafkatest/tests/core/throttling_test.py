# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time

from ducktape.tests.test import Test

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.performance import ProducerPerformanceService


class ThrottlingTest(Test):
    """This class provides all the system tests for replication throttling.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ThrottlingTest, self).__init__(test_context=test_context)
        self.topic = "test_topic_16Sep_1"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.kafka = KafkaService(test_context, num_nodes=6, zk=self.zk,
                                  security_protocol='PLAINTEXT',
                                  interbroker_security_protocol='PLAINTEXT',
                                  replication_throttling_rate=1000,
                                  topics={
                                      self.topic: {
                                          'partitions': 6,
                                          'replication-factor': 3,
                                          'replica-assignment': '0:1:2, 1:2:3, 2:3:4, 3:4:5, 4:5:0, 5:0:1',
                                          'configs': {
                                              'min.insync.replicas': 1,
                                              'quota.replication.throttled.replicas': '0:0,1:1,2:2,3:3'
                                          }
                                      }
                                  },
                                  jmx_object_names=['kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec',
                                                    'kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec'],
                                  jmx_attributes=['OneMinuteRate'])
        self.num_producers = 1
        self.num_consumers = 1
        self.num_records = 6000
        self.record_size = 100 * 1024  # 100 KB

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        """
        Override this since we're adding services outside of the constructor
        """
        return super(ThrottlingTest, self).min_cluster_size() +\
            self.num_producers + self.num_consumers


    def validate(self, broker, producer, consumer):
        success = True

        self.kafka.read_jmx_output_all_nodes()

        produced_num = sum([value['records'] for value in producer.results])
        consumed_num = sum([len(value) for value in consumer.messages_consumed.values()])
        self.logger.info("Producer produced %d messages" % produced_num)
        self.logger.info("Consumer consumed %d messages" % consumed_num)
        if produced_num != consumed_num:
            success = False

        return success

    def test_throttling_basic(self):
        security_protocol = 'PLAINTEXT'
        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol
        self.kafka.start_some([0,1,2,3])
        producer_num = 1
        producer_id = 'default_producer'
        producer = ProducerPerformanceService(
            self.test_context, producer_num, self.kafka, topic=self.topic,
            num_records=self.num_records, record_size=self.record_size,
            throughput=-1, client_id=producer_id,
            jmx_object_names=
            ['kafka.producer:type=producer-metrics,client-id=%s' % producer_id],
             jmx_attributes=['outgoing-byte-rate'])

        producer.run()

        consumer_id = 'default_consumer_id'
        consumer_num = 1

        consumer = ConsoleConsumer(self.test_context, consumer_num, self.kafka,
                                   self.topic, new_consumer=False,
                                   consumer_timeout_ms=60000,
                                   client_id=consumer_id,
                                   jmx_object_names=
                                   ['kafka.consumer:type=ConsumerTopicMetrics,name=BytesPerSec,clientId=%s' % consumer_id],
                                   jmx_attributes=['OneMinuteRate'])
        consumer.run()

        result = self.validate(self.kafka, producer, consumer)

        assert result

        # All the messages have been published properly. Let's start the
        # rest of the brokers.
        self.logger.debug("About to start remaining brokers..")
        self.kafka.start_some([4,5], create_topics=False)
        self.logger.debug("Remaining brokers started..")

        # TODO(apurva): Check that the throttled replicas catch up slower than the
        # un-throttled ones.
        #
        # partitions 0,1,2,3 are throttled on brokers 0,1,2,3 respectively.
        # What's left to do is to ensure that these brokers are the leaders for
        # these partitions.
        #
        # Then we have to make sure that if this is true, then these throttled
        # partitions catch up on brokers 4,5 at a slower rate than partitions
        # 4,5, and that the rate is consistent with the throttle of
        # 1000 Byte/s
        for partition in range(6):
            leader = self.kafka.idx(self.kafka.leader(self.topic, partition))
            try:
                self.logger.info("Leader for partition %d is %d" % (partition,
                                 leader))
            except:
                pass

        self.logger.info("Sleeping for 60 seconds")
        time.sleep(60)
