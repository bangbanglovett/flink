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

package org.apache.flink.table.runtime.operators.window.slicing;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link SliceAssigners.CumulativeSliceAssigner}. */
@RunWith(Parameterized.class)
public class CumulativeSliceAssignerTest extends SliceAssignerTestBase {

    @Parameterized.Parameter public TimeZone timeZone;

    @Parameterized.Parameters(name = "timezone = {0}")
    public static Collection<TimeZone> parameters() {
        return Arrays.asList(
                TimeZone.getTimeZone("America/Los_Angeles"), TimeZone.getTimeZone("Asia/Shanghai"));
    }

    @Test
    public void testSliceAssignment() {
        SliceAssigner assigner =
                SliceAssigners.cumulative(0, timeZone, Duration.ofDays(1), Duration.ofHours(1));

        assertEquals(
                epochMills("1970-01-01T01:00:00"),
                assignSliceEnd(assigner, epochMills("1970-01-01T00:00:00")));
        assertEquals(
                epochMills("1970-01-02T23:00:00"),
                assignSliceEnd(assigner, epochMills("1970-01-02T22:59:59.999")));
        assertEquals(
                epochMills("1970-01-03T00:00:00"),
                assignSliceEnd(assigner, epochMills("1970-01-02T23:00:00")));
    }

    @Test
    public void testSliceAssignmentWithOffset() {
        SliceAssigner assigner =
                SliceAssigners.cumulative(0, timeZone, Duration.ofHours(5), Duration.ofHours(1))
                        .withOffset(Duration.ofMillis(100));

        assertEquals(
                epochMills("1970-01-01T01:00:00.100"),
                assignSliceEnd(assigner, epochMills("1970-01-01T00:00:00.100")));
        assertEquals(
                epochMills("1970-01-01T05:00:00.100"),
                assignSliceEnd(assigner, epochMills("1970-01-01T05:00:00.099")));
        assertEquals(
                epochMills("1970-01-01T06:00:00.100"),
                assignSliceEnd(assigner, epochMills("1970-01-01T05:00:00.1")));
    }

    @Test
    public void testDstSaving() {
        if (!timeZone.useDaylightTime()) {
            return;
        }
        SliceAssigner assigner =
                SliceAssigners.cumulative(0, timeZone, Duration.ofHours(4), Duration.ofHours(1));

        // Los_Angeles local time in epoch mills.
        // The DaylightTime in Los_Angele start at time 2021-03-14 02:00:00
        long epoch1 = 1615708800000L; // 2021-03-14 00:00:00
        long epoch2 = 1615712400000L; // 2021-03-14 01:00:00
        long epoch3 = 1615716000000L; // 2021-03-14 03:00:00, skip one hour (2021-03-14 02:00:00)
        long epoch4 = 1615719600000L; // 2021-03-14 04:00:00

        assertSliceStartEnd("2021-03-14T00:00", "2021-03-14T01:00", epoch1, assigner, timeZone);
        assertSliceStartEnd("2021-03-14T00:00", "2021-03-14T03:00", epoch2, assigner, timeZone);
        assertSliceStartEnd("2021-03-14T00:00", "2021-03-14T04:00", epoch3, assigner, timeZone);
        assertSliceStartEnd("2021-03-14T04:00", "2021-03-14T05:00", epoch4, assigner, timeZone);

        // Los_Angeles local time in epoch mills.
        // The DaylightTime in Los_Angele end at time 2021-11-07 02:00:00
        long epoch5 = 1636268400000L; // 2021-11-07 00:00:00
        long epoch6 = 1636272000000L; // the first local timestamp 2021-11-07 01:00:00
        long epoch7 = 1636275600000L; // rollback to  2021-11-07 01:00:00
        long epoch8 = 1636279200000L; // 2021-11-07 02:00:00
        long epoch9 = 1636282800000L; // 2021-11-07 03:00:00
        long epoch10 = 1636286400000L; // 2021-11-07 04:00:00

        assertSliceStartEnd("2021-11-07T00:00", "2021-11-07T01:00", epoch5, assigner, timeZone);
        assertSliceStartEnd("2021-11-07T00:00", "2021-11-07T02:00", epoch6, assigner, timeZone);
        assertSliceStartEnd("2021-11-07T00:00", "2021-11-07T02:00", epoch7, assigner, timeZone);
        assertSliceStartEnd("2021-11-07T00:00", "2021-11-07T03:00", epoch8, assigner, timeZone);
        assertSliceStartEnd("2021-11-07T00:00", "2021-11-07T04:00", epoch9, assigner, timeZone);
        assertSliceStartEnd("2021-11-07T04:00", "2021-11-07T05:00", epoch10, assigner, timeZone);
    }

    @Test
    public void testGetWindowStart() {
        SliceAssigner assigner =
                SliceAssigners.cumulative(0, timeZone, Duration.ofHours(5), Duration.ofHours(1));

        assertEquals(
                epochMills("1969-12-31T19:00:00"),
                assigner.getWindowStart(epochMills("1970-01-01T00:00:00")));
        assertEquals(
                epochMills("1970-01-01T00:00:00"),
                assigner.getWindowStart(epochMills("1970-01-01T01:00:00")));
        assertEquals(
                epochMills("1970-01-01T00:00:00"),
                assigner.getWindowStart(epochMills("1970-01-01T02:00:00")));
        assertEquals(
                epochMills("1970-01-01T00:00:00"),
                assigner.getWindowStart(epochMills("1970-01-01T03:00:00")));
        assertEquals(
                epochMills("1970-01-01T00:00:00"),
                assigner.getWindowStart(epochMills("1970-01-01T04:00:00")));
        assertEquals(
                epochMills("1970-01-01T00:00:00"),
                assigner.getWindowStart(epochMills("1970-01-01T05:00:00")));
        assertEquals(
                epochMills("1970-01-01T05:00:00"),
                assigner.getWindowStart(epochMills("1970-01-01T06:00:00")));
        assertEquals(
                epochMills("1970-01-01T05:00:00"),
                assigner.getWindowStart(epochMills("1970-01-01T08:00:00")));
    }

    @Test
    public void testExpiredSlices() {
        SliceAssigner assigner =
                SliceAssigners.cumulative(0, timeZone, Duration.ofHours(5), Duration.ofHours(1));

        // reuse the first slice, skip to cleanup it
        assertEquals(
                Collections.emptyList(),
                expiredSlices(assigner, epochMills("1970-01-01T01:00:00")));

        assertEquals(
                Collections.singletonList(epochMills("1970-01-01T02:00:00")),
                expiredSlices(assigner, epochMills("1970-01-01T02:00:00")));
        assertEquals(
                Collections.singletonList(epochMills("1970-01-01T03:00:00")),
                expiredSlices(assigner, epochMills("1970-01-01T03:00:00")));
        assertEquals(
                Collections.singletonList(epochMills("1970-01-01T04:00:00")),
                expiredSlices(assigner, epochMills("1970-01-01T04:00:00")));
        assertEquals(
                Arrays.asList(epochMills("1970-01-01T05:00:00"), epochMills("1970-01-01T01:00:00")),
                expiredSlices(assigner, epochMills("1970-01-01T05:00:00")));

        // reuse the first slice, skip to cleanup it
        assertEquals(
                Collections.emptyList(),
                expiredSlices(assigner, epochMills("1970-01-01T06:00:00")));

        assertEquals(
                Arrays.asList(epochMills("1970-01-01T10:00:00"), epochMills("1970-01-01T06:00:00")),
                expiredSlices(assigner, epochMills("1970-01-01T10:00:00")));
        assertEquals(
                Arrays.asList(epochMills("1970-01-01T00:00:00"), epochMills("1969-12-31T20:00:00")),
                expiredSlices(assigner, epochMills("1970-01-01T00:00:00")));
    }

    @Test
    public void testMerge() throws Exception {
        SliceAssigners.CumulativeSliceAssigner assigner =
                SliceAssigners.cumulative(0, timeZone, Duration.ofHours(5), Duration.ofHours(1));

        assertEquals(
                Long.valueOf(epochMills("1970-01-01T01:00:00")),
                mergeResultSlice(assigner, epochMills("1970-01-01T01:00:00")));
        assertEquals(
                Collections.emptyList(),
                toBeMergedSlices(assigner, epochMills("1970-01-01T01:00:00"))); // the first slice

        assertEquals(
                Long.valueOf(epochMills("1970-01-01T01:00:00")),
                mergeResultSlice(assigner, epochMills("1970-01-01T02:00:00")));
        assertEquals(
                Collections.singletonList(epochMills("1970-01-01T02:00:00")),
                toBeMergedSlices(assigner, epochMills("1970-01-01T02:00:00")));

        assertEquals(
                Long.valueOf(epochMills("1970-01-01T01:00:00")),
                mergeResultSlice(assigner, epochMills("1970-01-01T03:00:00")));
        assertEquals(
                Collections.singletonList(epochMills("1970-01-01T03:00:00")),
                toBeMergedSlices(assigner, epochMills("1970-01-01T03:00:00")));

        assertEquals(
                Long.valueOf(epochMills("1970-01-01T01:00:00")),
                mergeResultSlice(assigner, epochMills("1970-01-01T04:00:00")));
        assertEquals(
                Collections.singletonList(epochMills("1970-01-01T04:00:00")),
                toBeMergedSlices(assigner, epochMills("1970-01-01T04:00:00")));

        assertEquals(
                Long.valueOf(epochMills("1970-01-01T01:00:00")),
                mergeResultSlice(assigner, epochMills("1970-01-01T05:00:00")));
        assertEquals(
                Collections.singletonList(epochMills("1970-01-01T05:00:00")),
                toBeMergedSlices(assigner, epochMills("1970-01-01T05:00:00")));

        assertEquals(
                Long.valueOf(epochMills("1970-01-01T06:00:00")),
                mergeResultSlice(assigner, epochMills("1970-01-01T06:00:00")));
        assertEquals(
                Collections.emptyList(),
                toBeMergedSlices(assigner, epochMills("1970-01-01T06:00:00"))); // the first slice

        assertEquals(
                Long.valueOf(epochMills("1970-01-01T06:00:00")),
                mergeResultSlice(assigner, epochMills("1970-01-01T08:00:00")));
        assertEquals(
                Collections.singletonList(epochMills("1970-01-01T08:00:00")),
                toBeMergedSlices(assigner, epochMills("1970-01-01T08:00:00")));

        assertEquals(
                Long.valueOf(epochMills("1970-01-01T06:00:00")),
                mergeResultSlice(assigner, epochMills("1970-01-01T10:00:00")));
        assertEquals(
                Collections.singletonList(epochMills("1970-01-01T10:00:00")),
                toBeMergedSlices(assigner, epochMills("1970-01-01T10:00:00")));

        assertEquals(
                Long.valueOf(epochMills("1969-12-31T20:00:00")),
                mergeResultSlice(assigner, epochMills("1970-01-01T00:00:00")));
        assertEquals(
                Collections.singletonList(epochMills("1970-01-01T00:00:00")),
                toBeMergedSlices(assigner, epochMills("1970-01-01T00:00:00")));
    }

    @Test
    public void testNextTriggerWindow() {
        SliceAssigners.CumulativeSliceAssigner assigner =
                SliceAssigners.cumulative(0, timeZone, Duration.ofHours(5), Duration.ofHours(1));

        assertEquals(
                Optional.empty(),
                assigner.nextTriggerWindow(epochMills("1970-01-01T00:00:00"), () -> false));
        assertEquals(
                Optional.of(epochMills("1970-01-01T02:00:00")),
                assigner.nextTriggerWindow(epochMills("1970-01-01T01:00:00"), () -> false));
        assertEquals(
                Optional.of(epochMills("1970-01-01T03:00:00")),
                assigner.nextTriggerWindow(epochMills("1970-01-01T02:00:00"), () -> false));
        assertEquals(
                Optional.of(epochMills("1970-01-01T04:00:00")),
                assigner.nextTriggerWindow(epochMills("1970-01-01T03:00:00"), () -> false));
        assertEquals(
                Optional.of(epochMills("1970-01-01T05:00:00")),
                assigner.nextTriggerWindow(epochMills("1970-01-01T04:00:00"), () -> false));
        assertEquals(
                Optional.empty(),
                assigner.nextTriggerWindow(epochMills("1970-01-01T05:00:00"), () -> false));
        assertEquals(
                Optional.of(epochMills("1970-01-01T07:00:00")),
                assigner.nextTriggerWindow(epochMills("1970-01-01T06:00:00"), () -> false));

        assertEquals(
                Optional.empty(),
                assigner.nextTriggerWindow(epochMills("1970-01-01T00:00:00"), () -> true));
        assertEquals(
                Optional.of(epochMills("1970-01-01T02:00:00")),
                assigner.nextTriggerWindow(epochMills("1970-01-01T01:00:00"), () -> true));
        assertEquals(
                Optional.of(epochMills("1970-01-01T03:00:00")),
                assigner.nextTriggerWindow(epochMills("1970-01-01T02:00:00"), () -> true));
        assertEquals(
                Optional.of(epochMills("1970-01-01T04:00:00")),
                assigner.nextTriggerWindow(epochMills("1970-01-01T03:00:00"), () -> true));
        assertEquals(
                Optional.of(epochMills("1970-01-01T05:00:00")),
                assigner.nextTriggerWindow(epochMills("1970-01-01T04:00:00"), () -> true));
        assertEquals(
                Optional.empty(),
                assigner.nextTriggerWindow(epochMills("1970-01-01T05:00:00"), () -> true));
        assertEquals(
                Optional.of(epochMills("1970-01-01T07:00:00")),
                assigner.nextTriggerWindow(epochMills("1970-01-01T06:00:00"), () -> true));
    }

    @Test
    public void testEventTime() {
        if (timeZone == TimeZone.getTimeZone("Asia/Shanghai")) {
            return;
        }
        SliceAssigner assigner1 =
                SliceAssigners.cumulative(
                        0, timeZone, Duration.ofSeconds(5), Duration.ofSeconds(1));
        assertTrue(assigner1.isEventTime());

        SliceAssigner assigner2 =
                SliceAssigners.cumulative(
                        -1, timeZone, Duration.ofSeconds(5), Duration.ofSeconds(1));
        assertFalse(assigner2.isEventTime());
    }

    @Test
    public void testInvalidParameters() {
        assertErrorMessage(
                () ->
                        SliceAssigners.cumulative(
                                0, timeZone, Duration.ofSeconds(-5), Duration.ofSeconds(1)),
                "Cumulative Window parameters must satisfy maxSize > 0 and step > 0, but got maxSize -5000ms and step 1000ms.");

        assertErrorMessage(
                () ->
                        SliceAssigners.cumulative(
                                0, timeZone, Duration.ofSeconds(5), Duration.ofSeconds(-1)),
                "Cumulative Window parameters must satisfy maxSize > 0 and step > 0, but got maxSize 5000ms and step -1000ms.");

        assertErrorMessage(
                () ->
                        SliceAssigners.cumulative(
                                0, timeZone, Duration.ofSeconds(5), Duration.ofSeconds(2)),
                "Cumulative Window requires maxSize must be an integral multiple of step, but got maxSize 5000ms and step 2000ms.");

        // should pass
        SliceAssigners.hopping(0, timeZone, Duration.ofSeconds(10), Duration.ofSeconds(2))
                .withOffset(Duration.ofSeconds(-1));
    }

    /** Get epoch mills from a timestamp string and the parameterized time zone. */
    private long epochMills(String timestampStr) {
        return epochMills(timeZone, timestampStr);
    }
}
