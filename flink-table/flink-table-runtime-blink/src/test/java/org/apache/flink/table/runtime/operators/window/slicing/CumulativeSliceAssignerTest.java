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
        return Arrays.asList(TimeZone.getTimeZone("UTC"), TimeZone.getTimeZone("Asia/Shanghai"));
    }

    @Test
    public void testSliceAssignment() {
        SliceAssigner assigner =
                SliceAssigners.cumulative(
                        0, timeZone.getRawOffset(), Duration.ofDays(1), Duration.ofHours(1));

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
                SliceAssigners.cumulative(
                                0,
                                timeZone.getRawOffset(),
                                Duration.ofHours(5),
                                Duration.ofHours(1))
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
    public void testGetWindowStart() {
        SliceAssigner assigner =
                SliceAssigners.cumulative(
                        0, timeZone.getRawOffset(), Duration.ofHours(5), Duration.ofHours(1));

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
                SliceAssigners.cumulative(
                        0, timeZone.getRawOffset(), Duration.ofHours(5), Duration.ofHours(1));

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
                SliceAssigners.cumulative(
                        0, timeZone.getRawOffset(), Duration.ofHours(5), Duration.ofHours(1));

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
                SliceAssigners.cumulative(
                        0, timeZone.getRawOffset(), Duration.ofHours(5), Duration.ofHours(1));

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
                        0, timeZone.getRawOffset(), Duration.ofSeconds(5), Duration.ofSeconds(1));
        assertTrue(assigner1.isEventTime());

        SliceAssigner assigner2 =
                SliceAssigners.cumulative(
                        -1, timeZone.getRawOffset(), Duration.ofSeconds(5), Duration.ofSeconds(1));
        assertFalse(assigner2.isEventTime());
    }

    @Test
    public void testInvalidParameters() {
        assertErrorMessage(
                () ->
                        SliceAssigners.cumulative(
                                0,
                                timeZone.getRawOffset(),
                                Duration.ofSeconds(-5),
                                Duration.ofSeconds(1)),
                "Cumulative Window parameters must satisfy maxSize > 0 and step > 0, but got maxSize -5000ms and step 1000ms.");

        assertErrorMessage(
                () ->
                        SliceAssigners.cumulative(
                                0,
                                timeZone.getRawOffset(),
                                Duration.ofSeconds(5),
                                Duration.ofSeconds(-1)),
                "Cumulative Window parameters must satisfy maxSize > 0 and step > 0, but got maxSize 5000ms and step -1000ms.");

        assertErrorMessage(
                () ->
                        SliceAssigners.cumulative(
                                timeZone.getRawOffset(),
                                0,
                                Duration.ofSeconds(5),
                                Duration.ofSeconds(2)),
                "Cumulative Window requires maxSize must be an integral multiple of step, but got maxSize 5000ms and step 2000ms.");

        // should pass
        SliceAssigners.hopping(
                        0, timeZone.getRawOffset(), Duration.ofSeconds(10), Duration.ofSeconds(2))
                .withOffset(Duration.ofSeconds(-1));
    }

    /** Get epoch mills from a timestamp string and the parameterized time zone. */
    private long epochMills(String timestampStr) {
        return epochMills(timeZone, timestampStr);
    }
}
