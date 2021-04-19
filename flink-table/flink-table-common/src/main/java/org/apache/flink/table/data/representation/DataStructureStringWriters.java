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

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.time.ZoneId;
import java.util.List;

/** Registry of available data structure string writers. */
public class DataStructureStringWriters {

    /** Returns a StringWriter for the given {@link DataType} and user configured time zone. */
    public static DataStructureStringWriter<Object> getDataStructureStringWriter(
            DataType dataType, ZoneId sessionTimeZone) {
        return (DataStructureStringWriter<Object>)
                getInternalDataStructureStringWriter(dataType, sessionTimeZone);
    }

    private static DataStructureStringWriter<?> getInternalDataStructureStringWriter(
            DataType dataType, ZoneId sessionTimeZone) {
        final LogicalTypeRoot typeRoot = dataType.getLogicalType().getTypeRoot();
        final Class<?> conversionClass = dataType.getConversionClass();
        switch (typeRoot) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (conversionClass.isAssignableFrom(java.sql.Timestamp.class)) {
                    // conversion between java.sql.Timestamp and TIMESTAMP_WITHOUT_TIME_ZONE
                    return TimestampTimestampStringWriter.create(dataType);
                } else if (conversionClass.isAssignableFrom(java.time.LocalDateTime.class)) {
                    return TimestampLocalDateTimeStringWriter.create(dataType);
                } else if (conversionClass.isAssignableFrom(TimestampData.class)) {
                    return TimestampTimestampDataStringWriter.create(dataType);
                } else {
                    return OriginStringWriter.create();
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (conversionClass.isAssignableFrom(java.time.Instant.class)) {
                    return LocalZonedTimestampInstantStringWriter.create(dataType, sessionTimeZone);
                } else if (conversionClass.isAssignableFrom(java.sql.Timestamp.class)) {
                    return LocalZonedTimestampTimestampStringWriter.create(
                            dataType, sessionTimeZone);
                } else if (conversionClass.isAssignableFrom(TimestampData.class)) {
                    return LocalZonedTimestampTimestampDataStringWriter.create(
                            dataType, sessionTimeZone);
                } else if (conversionClass.isAssignableFrom(Integer.class)
                        || conversionClass.isAssignableFrom(int.class)) {
                    return LocalZonedTimestampIntStringWriter.create(dataType, sessionTimeZone);
                } else if (conversionClass.isAssignableFrom(Long.class)
                        || conversionClass.isAssignableFrom(long.class)) {
                    return LocalZonedTimestampLongStringWriter.create(dataType, sessionTimeZone);
                }
            case DISTINCT_TYPE:
                return getInternalDataStructureStringWriter(
                        dataType.getChildren().get(0), sessionTimeZone);
            case ARRAY:
                if (List.class.isAssignableFrom(conversionClass)) {
                    return ArrayListStringWriter.create(dataType, sessionTimeZone);
                }
            case ROW:

            case MULTISET:
                // for subclasses of Map
                return MapMapConverter.createForMultisetType(dataType);
            case MAP:
                // for subclasses of Map
                return MapMapConverter.createForMapType(dataType);
                // TODO support STRUCTURED_TYPE
            default:
                return OriginStringWriter.create();
        }
    }

    private interface DataStructureStringWriterFactory {
        DataStructureStringWriter<?> createWriter(DataType dt, ZoneId sessionTimeZone);
    }
}
