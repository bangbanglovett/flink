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

package org.apache.flink.table.runtime.util;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.TimeZone;

/** Time util to deals window start and end in different timezone. */
public class TimeWindowUtil {

    private static final ZoneId UTC_ZONE_ID = TimeZone.getTimeZone("UTC").toZoneId();

    private static final long SECONDS_PER_HOUR = 60 * 60L;

    private static final long MILLS_PER_HOUR = SECONDS_PER_HOUR * 1000L;

    /**
     * Convert a epoch mills to timestamp mills which can describe a locate date time.
     *
     * <p>For example: The timestamp string of epoch mills 5 in UTC+8 is 1970-01-01 08:00:05, the
     * timestamp mills is 8 * 60 * 60 * 100 + 5.
     *
     * @param epochMills the epoch mills.
     * @param timeZone the timezone
     * @return the mills which can describe the local timestamp string in given timezone.
     */
    public static long toUtcTimestampMills(long epochMills, TimeZone timeZone) {
        if (timeZone.toZoneId().equals(UTC_ZONE_ID)) {
            return epochMills;
        }
        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMills), timeZone.toZoneId());
        return localDateTime.atZone(UTC_ZONE_ID).toInstant().toEpochMilli();
    }

    /**
     * Convert a timestamp mills with given timezone to epoch mills.
     *
     * @param utcTimestampMills the timestamp mills.
     * @param timeZone the timezone
     * @param usedInTimer if the utc timestamp can map to multiple epoch mills(at most two), use the
     *     bigger one for timer, use the smaller one for window property.
     * @return the epoch mills.
     */
    public static long toEpochMills(
            long utcTimestampMills, TimeZone timeZone, boolean usedInTimer) {
        if (timeZone.toZoneId().equals(UTC_ZONE_ID)) {
            return utcTimestampMills;
        }

        if (timeZone.useDaylightTime()) {
            // return the larger epoch mills if the time is leaving the DST.
            // eg. Los_Angeles has two timestamp 2021-11-07 01:00:00 when leaving DST.
            //  long epoch0  = 1636268400000L;  2021-11-07 00:00:00
            //  long epoch1  = 1636272000000L;  the first local timestamp 2021-11-07 01:00:00
            //  long epoch2  = 1636275600000L;  rollback to  2021-11-07 01:00:00
            //  long epoch3  = 1636279200000L;  2021-11-07 02:00:00
            // we should use the epoch2 to register timer to ensure the two hours' data can be fired
            // properly.
            LocalDateTime utcTimestamp =
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(utcTimestampMills), UTC_ZONE_ID);
            long epoch1 = utcTimestamp.atZone(timeZone.toZoneId()).toInstant().toEpochMilli();
            long epoch2 =
                    utcTimestamp
                            .plusSeconds(SECONDS_PER_HOUR)
                            .atZone(timeZone.toZoneId())
                            .toInstant()
                            .toEpochMilli();
            boolean hasTwoEpochs = epoch2 - epoch1 > MILLS_PER_HOUR;
            if (hasTwoEpochs && usedInTimer) {
                return epoch2;
            } else {
                return epoch1;
            }
        }

        LocalDateTime utcTimestamp =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(utcTimestampMills), UTC_ZONE_ID);
        return utcTimestamp.atZone(timeZone.toZoneId()).toInstant().toEpochMilli();
    }

    /**
     * Get the shifted time zone of window if time attribute type is TIMESTAMP_LTZ, always returns
     * UTC if the time attribute type is TIMESTAMP.
     */
    public static String getShiftTimeZone(LogicalType timeAttributeType, TableConfig tableConfig) {
        boolean needShiftTimeZone = LogicalTypeChecks.isTimestampLtzType(timeAttributeType);
        return needShiftTimeZone ? tableConfig.getLocalTimeZone().toString() : "UTC";
    }
}
