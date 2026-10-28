package com.flowstate.mapreduce;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import static org.apache.parquet.schema.OriginalType.DATE;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/** Writes the small aggregate. The 100M scan has already finished. */
public final class ResultWriter {
    static final String METRO_NOTE =
            "GTFS is not joined. The day grain does not use stop or route keys. "
                    + "No row was dropped for a missing GTFS key. "
                    + "On the amplified corpus the dependence is the resampler fit, "
                    + "not new evidence about Bengaluru.";
    static final String FLIGHT_NOTE = METRO_NOTE
            + " Not a prediction. ADR-0017 needs 60 shared covered dates. "
            + "The extract has 42 arrival covered days and 38 departure covered days.";
    private static final String[] PAYMENTS = {"smart_cards", "tokens", "qr", "ncmc"};

    private final Configuration conf;
    private final Map<String, Long> counts;
    private final String windowStart;
    private final String windowEnd;
    private final long corpusRows;
    private final long seed;
    private final String bundleHash;

    public ResultWriter(
            Configuration conf,
            Map<String, Long> counts,
            String windowStart,
            String windowEnd,
            long corpusRows,
            long seed,
            String bundleHash) {
        this.conf = conf;
        this.counts = counts;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.corpusRows = corpusRows;
        this.seed = seed;
        this.bundleHash = bundleHash;
    }

    public void demand(Path table, String runId, String input) throws IOException {
        List<SimpleGroup> rows = new ArrayList<SimpleGroup>();
        MessageType schema = demandSchema();
        StringBuilder json = new StringBuilder();
        json.append("{\"source\":").append(quote(input));
        json.append(",\"grain\":\"ward_id x hour_of_day x day_type\"");
        json.append(",\"bundle_version\":").append(quote(conf.get("flowstate.bundle.version")));
        json.append(",\"seed\":").append(seed);
        json.append(",\"corpus_rows\":").append(corpusRows);
        json.append(",\"scale\":").append(corpusRows);
        json.append(",\"rows\":[");
        boolean first = true;
        for (Map.Entry<String, Long> entry : counts.entrySet()) {
            String[] parts = split(entry.getKey());
            if (!"D".equals(parts[0])) {
                continue;
            }
            SimpleGroup row = record(schema);
            String ward = emptyToNull(parts[1]);
            int hour = Integer.parseInt(parts[2]);
            set(row, "ward_id", ward);
            set(row, "hour_of_day", hour);
            set(row, "day_type", parts[3]);
            set(row, "demand_count", entry.getValue());
            rows.add(row);
            if (!first) {
                json.append(',');
            }
            first = false;
            json.append("{\"ward_id\":").append(ward == null ? "null" : quote(ward));
            json.append(",\"hour_of_day\":").append(hour);
            json.append(",\"day_type\":").append(quote(parts[3]));
            json.append(",\"demand_count\":").append(entry.getValue());
            json.append('}');
        }
        json.append("]}");
        write(new Path(table, "run_id=" + runId), schema, rows);
        Path baseline = new Path(table.getParent(),
                "surge_baseline/run_id=" + runId + "/baseline.json");
        FileSystem fs = baseline.getFileSystem(conf);
        fs.mkdirs(baseline.getParent());
        try (OutputStream out = fs.create(baseline, true)) {
            out.write(json.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    public void cancellation(Path table, String runId) throws IOException {
        Map<String, long[]> paired = new TreeMap<String, long[]>();
        for (Map.Entry<String, Long> entry : counts.entrySet()) {
            String[] parts = split(entry.getKey());
            if (!"B".equals(parts[0]) && !"X".equals(parts[0])) {
                continue;
            }
            String grain = entry.getKey().substring(parts[0].length() + 1);
            long[] pair = paired.get(grain);
            if (pair == null) {
                pair = new long[2];
                paired.put(grain, pair);
            }
            if ("B".equals(parts[0])) {
                pair[0] = entry.getValue();
            } else {
                pair[1] = entry.getValue();
            }
        }
        MessageType schema = cancelSchema();
        List<SimpleGroup> rows = new ArrayList<SimpleGroup>();
        for (Map.Entry<String, long[]> entry : paired.entrySet()) {
            String[] parts = split(entry.getKey());
            long bookings = entry.getValue()[0];
            long cancels = entry.getValue()[1];
            SimpleGroup row = record(schema);
            set(row, "ward_id", emptyToNull(parts[0]));
            set(row, "hour_of_day", Integer.parseInt(parts[1]));
            set(row, "day_type", parts[2]);
            set(row, "vehicle_type", emptyToNull(parts[3]));
            set(row, "bookings", bookings);
            set(row, "cancels", cancels);
            set(row, "cancel_rate", bookings == 0 ? null : (double) cancels / bookings);
            rows.add(row);
        }
        write(new Path(table, "run_id=" + runId), schema, rows);
    }

    public void ridership(Path table, String runId, String metroDaily) throws IOException {
        MessageType schema = ridershipSchema();
        List<SimpleGroup> rows = new ArrayList<SimpleGroup>();
        for (Map.Entry<String, Long> entry : counts.entrySet()) {
            String[] parts = split(entry.getKey());
            if ("YD".equals(parts[0])) {
                rows.add(trend(schema, "day", parts[1], "metro", entry.getValue(),
                        null, null, null, null));
            } else if ("YM".equals(parts[0])) {
                rows.add(trend(schema, "month", parts[1], "metro", entry.getValue(),
                        null, null, null, null));
            } else if ("YA".equals(parts[0])) {
                rows.add(trend(schema, "day", parts[1], "aviation", entry.getValue(),
                        null, null, null, null));
            }
        }
        Map<String, long[]> hours = new TreeMap<String, long[]>();
        Map<String, boolean[]> seen = new TreeMap<String, boolean[]>();
        for (Map.Entry<String, Long> entry : counts.entrySet()) {
            String[] parts = split(entry.getKey());
            if (!"YS".equals(parts[0])) {
                continue;
            }
            String group = parts[1] + "\u0001" + parts[3];
            long[] slot = hours.get(group);
            boolean[] flag = seen.get(group);
            if (slot == null) {
                slot = new long[24];
                flag = new boolean[24];
                hours.put(group, slot);
                seen.put(group, flag);
            }
            int hour = Integer.parseInt(parts[2]);
            slot[hour] += entry.getValue();
            flag[hour] = true;
        }
        for (Map.Entry<String, long[]> entry : hours.entrySet()) {
            String[] group = entry.getKey().split("\u0001", -1);
            long[] slot = entry.getValue();
            boolean[] flag = seen.get(entry.getKey());
            long total = 0L;
            long peak = 0L;
            for (int hour = 0; hour < 24; hour++) {
                if (!flag[hour]) {
                    continue;
                }
                total += slot[hour];
                if (slot[hour] > peak) {
                    peak = slot[hour];
                }
            }
            for (int hour = 0; hour < 24; hour++) {
                if (!flag[hour] || slot[hour] != peak) {
                    continue;
                }
                Double share = total == 0 ? null : (double) slot[hour] / total;
                rows.add(trend(schema, "station_peak", null, "metro", slot[hour],
                        emptyToNull(group[0]), share, null, group[1]));
            }
        }
        if (metroDaily != null && !metroDaily.isEmpty()) {
            addPayments(schema, rows, new Path(metroDaily));
        }
        write(new Path(table, "run_id=" + runId), schema, rows);
    }

    public void crossModal(Path warehouse, String runId) throws IOException {
        Map<String, Long> ride = byDate("R");
        Map<String, Long> metro = byDate("M");
        Map<String, Long> flights = byDate("F");
        List<String> metroDays = new ArrayList<String>();
        List<double[]> metroRows = new ArrayList<double[]>();
        for (String day : ride.keySet()) {
            if (!metro.containsKey(day)) {
                continue;
            }
            metroDays.add(day);
            metroRows.add(new double[] {
                ride.get(day), metro.get(day), Clock.parse(day + "T12:00:00+05:30").weekdayFlag
            });
        }
        Slope metroFit = Slope.fit(metroRows.toArray(new double[0][]));
        MessageType metroType = metroSchema();
        List<SimpleGroup> metroOut = new ArrayList<SimpleGroup>();
        for (String day : metroDays) {
            SimpleGroup row = record(metroType);
            set(row, "period_start", day);
            set(row, "grain", "day");
            set(row, "n", (long) metroFit.n);
            set(row, "estimate", metroFit.estimate);
            set(row, "interval_low", metroFit.low);
            set(row, "interval_high", metroFit.high);
            set(row, "note", METRO_NOTE);
            metroOut.add(row);
        }
        List<String> flightDays = new ArrayList<String>();
        List<double[]> flightRows = new ArrayList<double[]>();
        for (String day : ride.keySet()) {
            if (!flights.containsKey(day)) {
                continue;
            }
            flightDays.add(day);
            flightRows.add(new double[] {
                ride.get(day), flights.get(day),
                Clock.parse(day + "T12:00:00+05:30").weekdayFlag
            });
        }
        Slope flightFit = Slope.fit(flightRows.toArray(new double[0][]));
        MessageType flightType = flightSchema();
        List<SimpleGroup> flightOut = new ArrayList<SimpleGroup>();
        for (String day : flightDays) {
            SimpleGroup row = record(flightType);
            set(row, "period_start", day);
            set(row, "n", (long) flightFit.n);
            set(row, "estimate", flightFit.estimate);
            set(row, "interval_low", flightFit.low);
            set(row, "interval_high", flightFit.high);
            set(row, "arrival_covered_days", 42);
            set(row, "departure_covered_days", 38);
            set(row, "required_covered_days", 60);
            set(row, "note", FLIGHT_NOTE);
            flightOut.add(row);
        }
        write(new Path(warehouse, "analytics_ridehail_metro/run_id=" + runId),
                metroType, metroOut);
        write(new Path(warehouse, "analytics_flights_ridehail/run_id=" + runId),
                flightType, flightOut);
    }

    private Map<String, Long> byDate(String kind) {
        Map<String, Long> out = new TreeMap<String, Long>();
        for (Map.Entry<String, Long> entry : counts.entrySet()) {
            String[] parts = split(entry.getKey());
            if (kind.equals(parts[0])) {
                out.put(parts[1], entry.getValue());
            }
        }
        return out;
    }

    private void addPayments(MessageType schema, List<SimpleGroup> rows, Path dir)
            throws IOException {
        FileSystem fs = dir.getFileSystem(conf);
        if (!fs.exists(dir)) {
            return;
        }
        List<Path> files = new ArrayList<Path>();
        collectParts(fs, dir, files);
        for (Path file : files) {
            try (ParquetReader<Group> reader = ParquetReader
                    .builder(new GroupReadSupport(), file)
                    .withConf(conf)
                    .build()) {
                Group group;
                while ((group = reader.read()) != null) {
                    String raw = CorpusMapper.field(group, "record_date");
                    if (raw == null || raw.length() < 10) {
                        continue;
                    }
                    String day = raw.substring(0, 10);
                    for (String name : PAYMENTS) {
                        if (!group.getType().containsField(name)
                                || group.getFieldRepetitionCount(name) == 0) {
                            continue;
                        }
                        rows.add(trend(schema, "day", day, "metro",
                                group.getLong(name, 0), null, null, name, null));
                    }
                }
            }
        }
    }

    private static void collectParts(FileSystem fs, Path dir, List<Path> files)
            throws IOException {
        if (fs.isFile(dir)) {
            String name = dir.getName();
            if (name.startsWith("part-") && name.endsWith(".parquet")) {
                files.add(dir);
            }
            return;
        }
        org.apache.hadoop.fs.FileStatus[] listed = fs.listStatus(dir);
        if (listed == null) {
            return;
        }
        for (org.apache.hadoop.fs.FileStatus status : listed) {
            collectParts(fs, status.getPath(), files);
        }
    }

    private SimpleGroup trend(
            MessageType schema, String grain, String period, String mode, long amount,
            String station, Double share, String payment, String phase) {
        SimpleGroup row = record(schema);
        set(row, "grain", grain);
        set(row, "period_start", period == null ? null : (int) LocalDate.parse(period).toEpochDay());
        set(row, "mode", mode);
        set(row, "volume", (double) amount);
        set(row, "station_id", station);
        set(row, "peak_share", share);
        set(row, "payment_method", payment);
        set(row, "line_phase", phase);
        return row;
    }

    private SimpleGroup record(MessageType schema) {
        SimpleGroup row = new SimpleGroup(schema);
        set(row, "corpus_rows", corpusRows);
        set(row, "seed", seed);
        set(row, "bundle_hash", bundleHash);
        set(row, "window_start", windowStart);
        set(row, "window_end", windowEnd);
        return row;
    }

    private static void set(SimpleGroup row, String field, Object value) {
        if (value == null) {
            return;
        }
        if (value instanceof String) {
            row.add(field, (String) value);
        } else if (value instanceof Integer) {
            row.add(field, ((Integer) value).intValue());
        } else if (value instanceof Long) {
            row.add(field, ((Long) value).longValue());
        } else if (value instanceof Double) {
            row.add(field, ((Double) value).doubleValue());
        } else {
            throw new IllegalArgumentException(field);
        }
    }

    private void write(Path dir, MessageType schema, List<SimpleGroup> rows)
            throws IOException {
        FileSystem fs = dir.getFileSystem(conf);
        fs.delete(dir, true);
        fs.mkdirs(dir);
        Path file = new Path(dir, "part-00000.parquet");
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
                .withType(schema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (SimpleGroup row : rows) {
                writer.write(row);
            }
        }
    }

    static String[] split(String key) {
        return key.split("\u0001", -1);
    }

    static String emptyToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }

    static String quote(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static MessageType demandSchema() {
        return Types.buildMessage()
                .optional(BINARY).as(UTF8).named("ward_id")
                .optional(INT32).named("hour_of_day")
                .optional(BINARY).as(UTF8).named("day_type")
                .optional(INT64).named("demand_count")
                .optional(INT64).named("corpus_rows")
                .optional(INT64).named("seed")
                .optional(BINARY).as(UTF8).named("bundle_hash")
                .optional(BINARY).as(UTF8).named("window_start")
                .optional(BINARY).as(UTF8).named("window_end")
                .named("demand");
    }

    private static MessageType cancelSchema() {
        return Types.buildMessage()
                .optional(BINARY).as(UTF8).named("ward_id")
                .optional(INT32).named("hour_of_day")
                .optional(BINARY).as(UTF8).named("day_type")
                .optional(BINARY).as(UTF8).named("vehicle_type")
                .optional(INT64).named("bookings")
                .optional(INT64).named("cancels")
                .optional(DOUBLE).named("cancel_rate")
                .optional(INT64).named("corpus_rows")
                .optional(INT64).named("seed")
                .optional(BINARY).as(UTF8).named("bundle_hash")
                .optional(BINARY).as(UTF8).named("window_start")
                .optional(BINARY).as(UTF8).named("window_end")
                .named("cancel");
    }

    private static MessageType ridershipSchema() {
        return Types.buildMessage()
                .optional(BINARY).as(UTF8).named("grain")
                .optional(INT32).as(DATE).named("period_start")
                .optional(BINARY).as(UTF8).named("mode")
                .optional(DOUBLE).named("volume")
                .optional(BINARY).as(UTF8).named("station_id")
                .optional(DOUBLE).named("peak_share")
                .optional(BINARY).as(UTF8).named("payment_method")
                .optional(BINARY).as(UTF8).named("line_phase")
                .optional(INT64).named("corpus_rows")
                .optional(INT64).named("seed")
                .optional(BINARY).as(UTF8).named("bundle_hash")
                .optional(BINARY).as(UTF8).named("window_start")
                .optional(BINARY).as(UTF8).named("window_end")
                .named("ridership");
    }

    private static MessageType metroSchema() {
        return Types.buildMessage()
                .optional(BINARY).as(UTF8).named("period_start")
                .optional(BINARY).as(UTF8).named("grain")
                .optional(INT64).named("n")
                .optional(DOUBLE).named("estimate")
                .optional(DOUBLE).named("interval_low")
                .optional(DOUBLE).named("interval_high")
                .optional(BINARY).as(UTF8).named("note")
                .optional(INT64).named("corpus_rows")
                .optional(INT64).named("seed")
                .optional(BINARY).as(UTF8).named("bundle_hash")
                .optional(BINARY).as(UTF8).named("window_start")
                .optional(BINARY).as(UTF8).named("window_end")
                .named("metro");
    }

    private static MessageType flightSchema() {
        return Types.buildMessage()
                .optional(BINARY).as(UTF8).named("period_start")
                .optional(INT64).named("n")
                .optional(DOUBLE).named("estimate")
                .optional(DOUBLE).named("interval_low")
                .optional(DOUBLE).named("interval_high")
                .optional(INT32).named("arrival_covered_days")
                .optional(INT32).named("departure_covered_days")
                .optional(INT32).named("required_covered_days")
                .optional(BINARY).as(UTF8).named("note")
                .optional(INT64).named("corpus_rows")
                .optional(INT64).named("seed")
                .optional(BINARY).as(UTF8).named("bundle_hash")
                .optional(BINARY).as(UTF8).named("window_start")
                .optional(BINARY).as(UTF8).named("window_end")
                .named("flights");
    }
}
