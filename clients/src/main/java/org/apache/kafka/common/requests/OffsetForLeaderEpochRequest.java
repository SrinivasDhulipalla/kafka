/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OffsetForLeaderEpochRequest extends AbstractRequest {
    public static final String TOPICS = "topics";
    public static final String TOPIC = "topic";
    public static final String PARTITIONS = "partitions";
    public static final String PARTITION_ID = "partition_id";
    public static final String LEADER_EPOCH = "leader_epoch";

    private Map<String, List<Epoch>> epochsByTopic;

    public Map<String, List<Epoch>> epochsByTopic() {
        return epochsByTopic;
    }


    public static class Builder extends AbstractRequest.Builder<OffsetForLeaderEpochRequest> {
        private Map<String, List<Epoch>> epochsByTopic;

        public Builder(Map<String, List<Epoch>> epochsByTopic) {
            super(ApiKeys.OFFSET_FOR_LEADER_EPOCH);
            this.epochsByTopic = epochsByTopic;
        }

        @Override
        public OffsetForLeaderEpochRequest build(short version) {
            return new OffsetForLeaderEpochRequest(epochsByTopic, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=OffsetForLeaderEpochRequest, ").
                    append("epochsByTopic=").append(epochsByTopic).
                    append(")");
            return bld.toString();
        }
    }

    public OffsetForLeaderEpochRequest(Map<String, List<Epoch>> epochsByTopic, short version) {
        super(version);
        this.epochsByTopic = epochsByTopic;
    }

    public OffsetForLeaderEpochRequest(Struct struct, short version) {
        super(version);
        epochsByTopic = new HashMap<>();
        for (Object t : struct.getArray(TOPICS)) {
            Struct topicAndEpochs = (Struct) t;
            String topic = topicAndEpochs.getString(TOPIC);
            List<Epoch> epochs = new ArrayList();
            for (Object e : topicAndEpochs.getArray(PARTITIONS)) {
                Struct partitionAndEpoch = (Struct) e;
                int partitionId = partitionAndEpoch.getInt(PARTITION_ID);
                int leaderEpoch = partitionAndEpoch.getInt(LEADER_EPOCH);
                epochs.add(new Epoch(partitionId, leaderEpoch));
            }
            epochsByTopic.put(topic, epochs);
        }
    }


    public static OffsetForLeaderEpochRequest parse(ByteBuffer buffer, short versionId) {
        return new OffsetForLeaderEpochRequest(ApiKeys.OFFSET_FOR_LEADER_EPOCH.parseRequest(versionId, buffer), versionId);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.OFFSET_FOR_LEADER_EPOCH.requestSchema(version()));
        List<Struct> topics = new ArrayList<>(epochsByTopic.size());
        for (Map.Entry<String, List<Epoch>> topicEpochs : epochsByTopic.entrySet()) {
            Struct partition = struct.instance(TOPICS);
            String topic = topicEpochs.getKey();
            partition.set(TOPIC, topic);
            List<Epoch> paritionEpochs = topicEpochs.getValue();
            List<Struct> paritions = new ArrayList<>(paritionEpochs.size());
            for (Epoch epoch : paritionEpochs) {
                Struct partitionRow = partition.instance(PARTITIONS);
                partitionRow.set(PARTITION_ID, epoch.partitionId());
                partitionRow.set(LEADER_EPOCH, epoch.epoch());
                paritions.add(partitionRow);
            }
            partition.set(PARTITIONS, paritions.toArray());
            topics.add(partition);
        }
        struct.set(TOPICS, topics.toArray());
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        return null;
    }
}
