package com.flowstate.mapreduce;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;

/** Asia/Kolkata clock. Monday–Friday is weekday. Matches demand.py. */
public final class Clock {
    public static final ZoneId ZONE = ZoneId.of("Asia/Kolkata");
    public static final LocalDate YELLOW_OPEN = LocalDate.of(2025, 8, 11);

    public final LocalDate date;
    public final int hour;
    public final String dayType;
    public final int weekdayFlag;

    private Clock(LocalDate date, int hour) {
        this.date = date;
        this.hour = hour;
        int mondayZero = date.getDayOfWeek().getValue() - 1;
        this.dayType = mondayZero <= 4 ? "weekday" : "weekend";
        this.weekdayFlag = mondayZero <= 4 ? 1 : 0;
    }

    public static Clock parse(String timestamp) {
        if (timestamp == null || timestamp.isEmpty()) {
            return null;
        }
        try {
            OffsetDateTime parsed = OffsetDateTime.parse(timestamp);
            LocalDate date = parsed.atZoneSameInstant(ZONE).toLocalDate();
            int hour = parsed.atZoneSameInstant(ZONE).getHour();
            return new Clock(date, hour);
        } catch (RuntimeException exc) {
            return null;
        }
    }

    public String phase() {
        return date.isBefore(YELLOW_OPEN) ? "before_open" : "after_open";
    }

    public String monthStart() {
        return date.withDayOfMonth(1).toString();
    }
}
