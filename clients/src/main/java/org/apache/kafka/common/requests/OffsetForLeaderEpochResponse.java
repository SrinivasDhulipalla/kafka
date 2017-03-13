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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.*;

public class OffsetForLeaderEpochResponse extends AbstractResponse {
    public static final String TOPICS = "topics";
    public static final String TOPIC = "topic";
    public static final String PARTITIONS = "partitions";
    public static final String ERROR_ID = "error_id";
    public static final String PARTITION_ID = "partition_id";
    public static final String END_OFFSET = "end_offset";

    private Map<String, List<EpochEndOffset>> epochsByTopic;

    public OffsetForLeaderEpochResponse(Struct struct) {
        epochsByTopic = new HashMap<>();
        for (Object t : struct.getArray(TOPICS)) {
            Struct topicAndEpochs = (Struct) t;
            String topic = topicAndEpochs.getString(TOPIC);
            List<EpochEndOffset> epochs = new ArrayList();
            for (Object e : topicAndEpochs.getArray(PARTITIONS)) {
                Struct partitionAndEpoch = (Struct) e;
                short errorId = partitionAndEpoch.getShort(ERROR_ID);
                int partitionId = partitionAndEpoch.getInt(PARTITION_ID);
                long endOffset = partitionAndEpoch.getLong(END_OFFSET);
                epochs.add(new EpochEndOffset(errorId, partitionId, endOffset));
            }
            epochsByTopic.put(topic, epochs);
        }
    }

    public OffsetForLeaderEpochResponse(Map<String, List<EpochEndOffset>> epochsByTopic) {
        this.epochsByTopic = epochsByTopic;
    }

    public Map<String, List<EpochEndOffset>> responses() {
        return epochsByTopic;
    }

    public static OffsetForLeaderEpochResponse parse(ByteBuffer buffer, short versionId) {
        return new OffsetForLeaderEpochResponse(ApiKeys.OFFSET_FOR_LEADER_EPOCH.parseResponse(versionId, buffer));
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.OFFSET_FOR_LEADER_EPOCH.responseSchema(version));
        List<Struct> topics = new ArrayList<>(epochsByTopic.size());
        for (Map.Entry<String, List<EpochEndOffset>> topicEpochs : epochsByTopic.entrySet()) {
            Struct partition = struct.instance(TOPICS);
            String topic = topicEpochs.getKey();
            partition.set(TOPIC, topic);
            List<EpochEndOffset> paritionEpochs = topicEpochs.getValue();
            List<Struct> paritions = new ArrayList<>(paritionEpochs.size());
            for (EpochEndOffset epoch : paritionEpochs) {
                Struct partitionRow = partition.instance(PARTITIONS);
                partitionRow.set(ERROR_ID, epoch.error());
                partitionRow.set(PARTITION_ID, epoch.partitionId());
                partitionRow.set(END_OFFSET, epoch.endOffset());
                paritions.add(partitionRow);
            }

            partition.set(PARTITIONS, paritions.toArray());
            topics.add(partition);
        }
        struct.set(TOPICS, topics.toArray());
        return struct;
    }
}
