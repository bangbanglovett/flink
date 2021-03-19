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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.TimeZone;

/** Time util to deals window start and end in different timezone. */
public class TimeWindowUtil {

    private static final ZoneId UTC_ZONE_ID = TimeZone.getTimeZone("UTC").toZoneId();

    /**
     * Method to get the window start for a timestamp.
     *
     * @param timestamp epoch millisecond to get the window start.
     * @param offset The offset which window start would be shifted by.
     * @param windowSize The size of the generated windows.
     * @param timezone The timeZone used to shift the window start.
     * @return window start
     */
    public static long getWindowStartWithOffset(
            long timestamp, long offset, long windowSize, TimeZone timezone) {
        if (timezone.useDaylightTime()) {
            // mapping to UTC timestamp string
            long timestampMills = TimeWindowUtil.toTimestampMills(timestamp, timezone);
            // calculate the window start in UTC
            long utcWindStart =
                    timestampMills
                            - ((timestampMills - offset) % windowSize + windowSize) % windowSize;
            // mapping back from UTC timestamp
            return TimeWindowUtil.toEpochMills(utcWindStart, timezone);
        } else {
            int timeZoneOffset = timezone.getOffset(timestamp);
            return timestamp
                    - ((timestamp + timeZoneOffset - offset) % windowSize + windowSize)
                            % windowSize;
        }
    }

    /** Minus an interval for window boundary, considering the timeZone and daylight savings. */
    public static long windowMinus(long windowBoundary, long interval, TimeZone timeZone) {
        if (timeZone.useDaylightTime()) {
            long utcMills = toTimestampMills(windowBoundary, timeZone);
            return toEpochMills(utcMills - interval, timeZone);
        } else {
            return windowBoundary - interval;
        }
    }

    /** Plus an interval for window boundary, considering the timeZone and daylight savings. */
    public static long windowPlus(long windowBoundary, long interval, TimeZone timeZone) {
        if (timeZone.useDaylightTime()) {
            long utcMills = toTimestampMills(windowBoundary, timeZone);
            return toEpochMills(utcMills + interval, timeZone);
        } else {
            return windowBoundary + interval;
        }
    }

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
    public static long toTimestampMills(long epochMills, TimeZone timeZone) {
        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMills), timeZone.toZoneId());
        return localDateTime.atZone(UTC_ZONE_ID).toInstant().toEpochMilli();
    }

    /**
     * Convert a timestamp mills with given timezone to epoch mills.
     *
     * @param timestampMills the timestamp mills.
     * @param timeZone the timezone
     * @return the epoch mills.
     */
    public static long toEpochMills(long timestampMills, TimeZone timeZone) {
        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timestampMills), UTC_ZONE_ID);
        return localDateTime.atZone(timeZone.toZoneId()).toInstant().toEpochMilli();
    }
}
