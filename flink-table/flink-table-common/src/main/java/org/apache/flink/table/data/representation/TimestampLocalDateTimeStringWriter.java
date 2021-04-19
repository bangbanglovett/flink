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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.utils.TimestampStringUtils;

import java.time.LocalDateTime;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

/** StringWriter for {@link TimestampType} of {@link LocalDateTime} external type. */
@Internal
public class TimestampLocalDateTimeStringWriter
        implements DataStructureStringWriter<LocalDateTime> {

    private static final long serialVersionUID = 1L;
    private final int precision;

    private TimestampLocalDateTimeStringWriter(int precision) {
        this.precision = precision;
    }

    @Override
    public String representString(LocalDateTime data) {
        return TimestampStringUtils.timestampToString(data, precision);
    }

    @SuppressWarnings({"unchecked"})
    public static DataStructureStringWriter create(DataType dataType) {
        return new TimestampLocalDateTimeStringWriter(getPrecision(dataType.getLogicalType()));
    }
}
