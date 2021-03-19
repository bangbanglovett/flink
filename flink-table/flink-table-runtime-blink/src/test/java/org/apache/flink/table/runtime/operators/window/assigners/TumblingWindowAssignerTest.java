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

package org.apache.flink.table.runtime.operators.window.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.TimeZone;

import static org.apache.flink.table.runtime.operators.window.WindowTestUtils.timeWindow;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for {@link TumblingWindowAssigner}. */
@RunWith(Parameterized.class)
public class TumblingWindowAssignerTest {

    public static final long HOUR = 3600000L; // = 60 * 60 * 1000

    @Parameterized.Parameter public TimeZone timeZone;

    @Parameterized.Parameters(name = "timezone = {0}")
    public static Collection<TimeZone> parameters() {
        return Arrays.asList(
                TimeZone.getTimeZone("America/Los_Angeles"), TimeZone.getTimeZone("Asia/Shanghai"));
    }

    private static final RowData ELEMENT = GenericRowData.of("String");
    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testWindowAssignment() {
        TumblingWindowAssigner assigner =
                TumblingWindowAssigner.of(Duration.ofMillis(5000), timeZone);
        assertThat(assigner.assignWindows(ELEMENT, 0L), contains(timeWindow(0, 5000)));
        assertThat(assigner.assignWindows(ELEMENT, 4999L), contains(timeWindow(0, 5000)));
        assertThat(assigner.assignWindows(ELEMENT, 5000L), contains(timeWindow(5000, 10000)));
    }

    @Test
    public void testWindowAssignmentWithOffset() {
        TumblingWindowAssigner assigner =
                TumblingWindowAssigner.of(Duration.ofMillis(5000), timeZone)
                        .withOffset(Duration.ofMillis(100));

        assertThat(assigner.assignWindows(ELEMENT, 100L), contains(timeWindow(100, 5100)));
        assertThat(assigner.assignWindows(ELEMENT, 5099L), contains(timeWindow(100, 5100)));
        assertThat(assigner.assignWindows(ELEMENT, 5100L), contains(timeWindow(5100, 10100)));
    }

    @Test
    public void testDstSaving() {
        if (!timeZone.useDaylightTime()) {
            return;
        }
        TumblingWindowAssigner assigner = TumblingWindowAssigner.of(Duration.ofHours(4), timeZone);
        // Los_Angeles local time in epoch mills.
        // The DaylightTime in Los_Angele start at time 2021-03-14 02:00:00
        long epoch1 = 1615708800000L; // 2021-03-14 00:00:00
        long epoch2 = 1615712400000L; // 2021-03-14 01:00:00
        long epoch3 = 1615716000000L; // 2021-03-14 03:00:00, skip one hour (2021-03-14 02:00:00)
        long epoch4 = 1615719600000L; // 2021-03-14 04:00:00

        assertThat(
                assigner.assignWindows(ELEMENT, epoch1),
                contains(timeWindow(epoch1, epoch1 + 3 * HOUR)));
        assertThat(
                assigner.assignWindows(ELEMENT, epoch2),
                contains(timeWindow(epoch1, epoch1 + 3 * HOUR)));
        assertThat(
                assigner.assignWindows(ELEMENT, epoch3),
                contains(timeWindow(epoch1, epoch1 + 3 * HOUR)));
        assertThat(
                assigner.assignWindows(ELEMENT, epoch4),
                contains(timeWindow(epoch4, epoch4 + 4 * HOUR)));

        // Los_Angeles local time in epoch mills.
        // The DaylightTime in Los_Angele end at time 2021-11-07 02:00:00
        long epoch5 = 1636268400000L; // 2021-11-07 00:00:00
        long epoch6 = 1636272000000L; // the first local timestamp 2021-11-07 01:00:00
        long epoch7 = 1636275600000L; // rollback to  2021-11-07 01:00:00
        long epoch8 = 1636279200000L; // 2021-11-07 02:00:00
        long epoch9 = 1636282800000L; // 2021-11-07 03:00:00
        long epoch10 = 1636286400000L; // 2021-11-07 04:00:00
        assertThat(
                assigner.assignWindows(ELEMENT, epoch5),
                contains(timeWindow(epoch5, epoch5 + 5 * HOUR)));
        assertThat(
                assigner.assignWindows(ELEMENT, epoch6),
                contains(timeWindow(epoch5, epoch5 + 5 * HOUR)));
        assertThat(
                assigner.assignWindows(ELEMENT, epoch7),
                contains(timeWindow(epoch5, epoch5 + 5 * HOUR)));
        assertThat(
                assigner.assignWindows(ELEMENT, epoch8),
                contains(timeWindow(epoch5, epoch5 + 5 * HOUR)));
        assertThat(
                assigner.assignWindows(ELEMENT, epoch9),
                contains(timeWindow(epoch5, epoch5 + 5 * HOUR)));
        assertThat(
                assigner.assignWindows(ELEMENT, epoch10),
                contains(timeWindow(epoch10, epoch10 + 4 * HOUR)));
    }

    @Test
    public void testInvalidParameters() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("size > 0");
        TumblingWindowAssigner.of(Duration.ofSeconds(-1), timeZone);

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("size > 0");
        TumblingWindowAssigner.of(Duration.ofSeconds(10), timeZone)
                .withOffset(Duration.ofSeconds(20));

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("size > 0");
        TumblingWindowAssigner.of(Duration.ofSeconds(10), timeZone)
                .withOffset(Duration.ofSeconds(-1));
    }

    @Test
    public void testProperties() {
        TumblingWindowAssigner assigner =
                TumblingWindowAssigner.of(Duration.ofMillis(5000), timeZone);

        assertTrue(assigner.isEventTime());
        assertEquals(
                new TimeWindow.Serializer(), assigner.getWindowSerializer(new ExecutionConfig()));

        assertTrue(assigner.withEventTime().isEventTime());
        assertFalse(assigner.withProcessingTime().isEventTime());
    }
}
