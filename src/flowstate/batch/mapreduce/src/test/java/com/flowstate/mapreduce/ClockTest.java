package com.flowstate.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class ClockTest {
    @Test
    public void kolkataOffsetKeepsTheLocalHour() {
        Clock clock = Clock.parse("2024-10-26T01:29:07+05:30");
        assertEquals("2024-10-26", clock.date.toString());
        assertEquals(1, clock.hour);
        assertEquals("weekend", clock.dayType);
        assertEquals(0, clock.weekdayFlag);
    }

    @Test
    public void mondayIsAWeekday() {
        Clock clock = Clock.parse("2024-10-28T18:00:00+05:30");
        assertEquals("weekday", clock.dayType);
        assertEquals(1, clock.weekdayFlag);
    }

    @Test
    public void yellowLineOpensOnTheDay() {
        assertEquals("before_open", Clock.parse("2025-08-10T12:00:00+05:30").phase());
        assertEquals("after_open", Clock.parse("2025-08-11T00:00:00+05:30").phase());
    }

    @Test
    public void aBlankTimestampIsSkipped() {
        assertNull(Clock.parse(null));
        assertNull(Clock.parse("not-a-time"));
    }
}
