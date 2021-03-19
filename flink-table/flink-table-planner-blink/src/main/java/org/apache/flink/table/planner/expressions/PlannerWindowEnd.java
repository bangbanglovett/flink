/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.expressions;

import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

/** Window end property. */
@JsonTypeName("WindowEnd")
public class PlannerWindowEnd extends AbstractPlannerWindowProperty {

    @JsonCreator
    public PlannerWindowEnd(@JsonProperty(FIELD_NAME_REFERENCE) PlannerWindowReference reference) {
        super(reference);
    }

    @Override
    public LogicalType getResultType() {
        if (reference.getType().isPresent()
                && reference.getType().get().getTypeRoot()
                        == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return new LocalZonedTimestampType(true, 3);
        } else {
            return new TimestampType(true, 3);
        }
    }

    @Override
    public String toString() {
        return String.format("end(%s)", reference);
    }
}
