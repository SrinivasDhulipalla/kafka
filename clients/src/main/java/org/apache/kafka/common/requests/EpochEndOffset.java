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
package org.apache.kafka.common.requests;

/**
 * Data Transfer Object for the  Offsets for Leader Epoch Response.
 */

public class EpochEndOffset {
    private short error;
    private int partitionId;
    private long endOffset;

    public EpochEndOffset(short error, int partitionId, long endOffset) {
        this.error = error;
        this.partitionId = partitionId;
        this.endOffset = endOffset;
    }

    public short error() {
        return error;
    }

    public boolean hasError() {
        return error > 0;
    }

    public int partitionId() {
        return partitionId;
    }

    public long endOffset() {
        return endOffset;
    }

    @Override
    public String toString() {
        return "EpochEndOffset{" +
                "error=" + error +
                ", partitionId=" + partitionId +
                ", endOffset=" + endOffset +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EpochEndOffset that = (EpochEndOffset) o;

        if (error != that.error) return false;
        if (partitionId != that.partitionId) return false;
        return endOffset == that.endOffset;

    }

    @Override
    public int hashCode() {
        int result = (int) error;
        result = 31 * result + partitionId;
        result = 31 * result + (int) (endOffset ^ (endOffset >>> 32));
        return result;
    }
}
