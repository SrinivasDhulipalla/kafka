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
import math
from ducktape.mark import parametrize
from ducktape.utils.util import wait_until


from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.utils import is_int_with_prefix
import random


class ThrottlingTest(ProduceConsumeValidateTest):
    """
    These tests validate partition reassignment.
    Create a topic with few partitions, load some data, trigger partition
    re-assignment with and without broker failure, check that partition
    re-assignment can complete and there is no data loss.
    """

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ThrottlingTest, self).__init__(test_context=test_context)

        self.topic = "test_topic"
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.num_brokers = 3
        self.kafka = KafkaService(test_context,
                                  num_nodes=self.num_brokers,
                                  zk=self.zk,
                                  topics={
                                      self.topic: {
                                          "partitions": 6,
                                          "replication-factor": 3,
                                          'configs': {"min.insync.replicas": 2}}
                                  })
        self.producer_throughput = 10000
        self.num_partitions = 6
        self.timeout_sec = 400
        self.num_records = 1000000
        avg_record_val = (self.num_records + 1) / 2
        self.record_size = int(math.log10(avg_record_val)) + 1
        # 1 MB per partition on average.
        self.partition_size = (self.num_records * self.record_size) / self.num_partitions
        self.num_producers = 1
        self.num_consumers = 1
        self.throttle = 2048  # 2 KB/s

    def setUp(self):
        self.zk.start()

    def min_cluster_size(self):
        # Override this since we're adding services outside of the constructor
        return super(ThrottlingTest, self).min_cluster_size() +\
            self.num_producers + self.num_consumers

    def clean_bounce_some_brokers(self):
        """Bounce every other broker"""
        for node in self.kafka.nodes[::2]:
            self.kafka.restart_node(node, clean_shutdown=True)

    def reassign_partitions(self, bounce_brokers, throttle):
        partition_info = self.kafka.parse_describe_topic(
            self.kafka.describe_topic(self.topic))
        self.logger.debug("Partitions before reassignment:" +
                          str(partition_info))

        # jumble partition assignment in dictionary
        seed = random.randint(0, 2 ** 31 - 1)
        self.logger.debug("Jumble partition assignment with seed " + str(seed))
        random.seed(seed)
        # The list may still be in order, but that's ok
        shuffled_list = range(0, self.num_partitions)
        random.shuffle(shuffled_list)

        num_moves = 0
        for i in range(0, self.num_partitions):
            if partition_info["partitions"][i]["partition"] != shuffled_list[i]:
                num_moves += 1
            partition_info["partitions"][i]["partition"] = shuffled_list[i]
        self.logger.debug("Jumbled partitions: " + str(partition_info))
        self.logger.info("Number of moves: %d", num_moves)
        # send reassign partitions command
        self.kafka.execute_reassign_partitions(partition_info, throttle)

        if bounce_brokers:
            # bounce a few brokers at the same time
            self.clean_bounce_some_brokers()

        # Wait until finished or timeout
        size_per_broker = (num_moves / self.num_brokers) * self.partition_size
        estimated_throttled_time = size_per_broker / self.throttle
        start = time.time()
        self.logger.info("Waiting %ds for the reassignment to complete",
                         estimated_throttled_time * 2)
        wait_until(lambda: self.kafka.verify_reassign_partitions(partition_info),
                   timeout_sec=estimated_throttled_time * 2, backoff_sec=.5)
        stop = time.time()
        self.logger.info("Transfer took %d second. Estimated time : %ds", stop - start,
                         estimated_throttled_time)

    @parametrize(security_protocol="PLAINTEXT", bounce_brokers=False)
    @parametrize(security_protocol="PLAINTEXT", bounce_brokers=True)
    def test_throttled_reassignment(self, bounce_brokers, security_protocol):
        """Reassign partitions tests.
        Setup: 1 zk, 3 kafka nodes, 1 topic with partitions=3,
        replication-factor=3, and min.insync.replicas=2

            - Produce messages in the background
            - Consume messages in the background
            - Reassign partitions
            - If bounce_brokers is True, also bounce a few brokers while
              partition re-assignment is in progress
            - When done reassigning partitions and bouncing brokers, stop
              producing, and finish consuming
            - Validate that every acked message was consumed
        """

        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol
        new_consumer = (False if self.kafka.security_protocol == "PLAINTEXT"
                        else True)
        self.producer = VerifiableProducer(self.test_context, self.num_producers,
                                           self.kafka, self.topic,
                                           message_validator = is_int_with_prefix,
                                           max_messages = self.num_records)

        self.consumer = ConsoleConsumer(self.test_context,
                                        self.num_consumers,
                                        self.kafka,
                                        self.topic,
                                        new_consumer=new_consumer,
                                        consumer_timeout_ms=60000,
                                        message_validator=is_int)
        self.kafka.start()
        self.run_produce_consume_validate(core_test_action=
                                          lambda: self.reassign_partitions(
                                              bounce_brokers, self.throttle))
