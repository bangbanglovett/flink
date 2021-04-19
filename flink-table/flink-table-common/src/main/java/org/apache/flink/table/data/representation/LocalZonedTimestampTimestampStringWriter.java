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

package org.apache.flink.table.data.representation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.utils.TimestampStringUtils;

import java.sql.Timestamp;
import java.time.ZoneId;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

/** StringWriter for {@link LocalZonedTimestampType} of {@link Timestamp} external type. */
@Internal
public class LocalZonedTimestampTimestampStringWriter
        implements DataStructureStringWriter<Timestamp> {

    private static final long serialVersionUID = 1L;
    private static final int NANOS_PER_MILL = 1000_000;

    private final ZoneId sessionTimeZone;
    private final int precision;

    public LocalZonedTimestampTimestampStringWriter(ZoneId sessionTimeZone, int precision) {
        this.sessionTimeZone = sessionTimeZone;
        this.precision = precision;
    }

    @Override
    public String representString(Timestamp data) {
        return TimestampStringUtils.timestampToString(
                TimestampData.fromEpochMillis(data.getTime(), data.getNanos() % NANOS_PER_MILL)
                        .toInstant()
                        .atZone(sessionTimeZone)
                        .toLocalDateTime(),
                precision);
    }

    @SuppressWarnings({"unchecked"})
    public static DataStructureStringWriter create(DataType dataType, ZoneId sessionTimeZone) {
        return new LocalZonedTimestampTimestampStringWriter(
                sessionTimeZone, getPrecision(dataType.getLogicalType()));
    }
}
