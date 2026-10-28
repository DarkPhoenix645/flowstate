package com.flowstate.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;

/**
 * One corpus row in, partial counts out. No row is kept on the heap.
 * The combiner sums the partials, so the shuffle stays the grain size
 * at 100M as well as at 1M.
 */
public class CorpusMapper extends Mapper<Void, Group, Text, LongWritable> {
    static final char SEP = '\u0001';

    private String job;
    private final Text key = new Text();
    private final LongWritable one = new LongWritable(1L);
    private final LongWritable volume = new LongWritable();

    @Override
    protected void setup(Context context) {
        job = context.getConfiguration().get("flowstate.job");
    }

    @Override
    protected void map(Void ignored, Group row, Context context)
            throws IOException, InterruptedException {
        String city = field(row, "city");
        if (city != null && !city.isEmpty() && !"bengaluru".equals(city)) {
            return;
        }
        Clock clock = Clock.parse(field(row, "timestamp"));
        if (clock == null) {
            context.getCounter("flowstate", "bad_timestamp").increment(1L);
            return;
        }
        String mode = field(row, "mode");
        String date = clock.date.toString();
        if ("demand_hotspots".equals(job) || "cancellation_analysis".equals(job)) {
            if (!"ride_hail".equals(mode)) {
                return;
            }
            emit(context, "W", "ride", date, 1L);
        } else {
            emit(context, "W", "all", date, 1L);
        }
        if ("demand_hotspots".equals(job)) {
            emit(context, "D", token(field(row, "pickup_zone")),
                    Integer.toString(clock.hour), clock.dayType, 1L);
            return;
        }
        if ("cancellation_analysis".equals(job)) {
            String grain = join(token(field(row, "pickup_zone")),
                    Integer.toString(clock.hour), clock.dayType,
                    token(field(row, "vehicle_type")));
            emit(context, "B", grain, 1L);
            if ("cancelled".equals(field(row, "status"))) {
                emit(context, "X", grain, 1L);
            }
            return;
        }
        if ("ridership_trends".equals(job)) {
            if ("metro".equals(mode)) {
                long count = longOrZero(row, "entries_count")
                        + longOrZero(row, "exits_count");
                emit(context, "YD", date, count);
                emit(context, "YM", clock.monthStart(), count);
                emit(context, "YS", token(field(row, "pickup_zone")),
                        Integer.toString(clock.hour), clock.phase(), count);
            } else if ("aviation".equals(mode)) {
                emit(context, "YA", date, 1L);
            }
            return;
        }
        if ("cross_modal".equals(job)) {
            if ("ride_hail".equals(mode)) {
                emit(context, "R", date, 1L);
            } else if ("metro".equals(mode)) {
                emit(context, "M", date,
                        longOrZero(row, "entries_count")
                                + longOrZero(row, "exits_count"));
            } else if ("aviation".equals(mode)) {
                emit(context, "F", date, 1L);
            }
        }
    }

    private void emit(Context context, String kind, String rest, long amount)
            throws IOException, InterruptedException {
        key.set(kind + SEP + rest);
        volume.set(amount);
        context.write(key, amount == 1L ? one : volume);
    }

    private void emit(Context context, String kind, String a, String b, long amount)
            throws IOException, InterruptedException {
        emit(context, kind, a + SEP + b, amount);
    }

    private void emit(
            Context context, String kind, String a, String b, String c, long amount)
            throws IOException, InterruptedException {
        emit(context, kind, a + SEP + b + SEP + c, amount);
    }

    static String join(String... parts) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                out.append(SEP);
            }
            out.append(parts[i]);
        }
        return out.toString();
    }

    static String token(String value) {
        return value == null ? "" : value;
    }

    static String field(Group row, String name) {
        if (!row.getType().containsField(name)
                || row.getFieldRepetitionCount(name) == 0) {
            return null;
        }
        String value = row.getString(name, 0);
        return value.isEmpty() ? null : value;
    }

    static long longOrZero(Group row, String name) {
        if (!row.getType().containsField(name)
                || row.getFieldRepetitionCount(name) == 0) {
            return 0L;
        }
        return row.getLong(name, 0);
    }
}
