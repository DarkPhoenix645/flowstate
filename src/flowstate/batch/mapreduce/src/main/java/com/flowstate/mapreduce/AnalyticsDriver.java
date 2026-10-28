package com.flowstate.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;

/**
 * Q1–Q5 on YARN. Input and output are URIs from the submitter.
 * Compose passes hdfs://. Dataproc (#72) passes gs:// for the 100M bench.
 * This class does not set container memory and does not name a cluster.
 */
public class AnalyticsDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new AnalyticsDriver(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Map<String, String> flags = flags(args);
        String jobName = required(flags, "job");
        String input = required(flags, "input");
        String output = required(flags, "output");
        String runId = required(flags, "run-id");
        long corpusRows = Long.parseLong(required(flags, "corpus-rows"));
        long seed = Long.parseLong(required(flags, "seed"));
        String bundleHash = required(flags, "bundle-hash");
        String bundleVersion = required(flags, "bundle-version");
        Configuration conf = getConf();
        conf.set("flowstate.job", jobName);
        conf.set("flowstate.bundle.version", bundleVersion);
        conf.setBoolean("mapreduce.job.user.classpath.first", true);
        Job job = Job.getInstance(conf, "flowstate-" + jobName);
        job.setJarByClass(AnalyticsDriver.class);
        job.setMapperClass(CorpusMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(ParquetInputFormat.class);
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
        FileInputFormat.setInputPathFilter(job, PartFilter.class);
        FileInputFormat.addInputPath(job, new Path(input));
        Path table = new Path(output);
        Path tmp = new Path(table.getParent(), "_mr_tmp/" + runId);
        FileSystem fs = tmp.getFileSystem(conf);
        fs.delete(tmp, true);
        FileOutputFormat.setOutputPath(job, tmp);
        if (!job.waitForCompletion(true)) {
            return 1;
        }
        try {
            Map<String, Long> counts = read(fs, tmp);
            String[] window = window(counts, jobName);
            ResultWriter writer = new ResultWriter(
                    conf, counts, window[0], window[1], corpusRows, seed, bundleHash);
            if ("demand_hotspots".equals(jobName)) {
                writer.demand(table, runId, input);
            } else if ("cancellation_analysis".equals(jobName)) {
                writer.cancellation(table, runId);
            } else if ("ridership_trends".equals(jobName)) {
                writer.ridership(table, runId, flags.get("metro-daily"));
            } else if ("cross_modal".equals(jobName)) {
                writer.crossModal(table, runId);
            } else {
                throw new IllegalArgumentException("unknown job " + jobName);
            }
            System.out.println("WROTE " + output + " run_id=" + runId);
            return 0;
        } finally {
            fs.delete(tmp, true);
        }
    }

    private static Map<String, Long> read(FileSystem fs, Path tmp) throws IOException {
        Map<String, Long> counts = new HashMap<String, Long>();
        if (!fs.exists(tmp)) {
            return counts;
        }
        for (FileStatus status : fs.listStatus(tmp)) {
            String name = status.getPath().getName();
            if (!name.startsWith("part")) {
                continue;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    fs.open(status.getPath()), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    int tab = line.lastIndexOf('\t');
                    if (tab < 0) {
                        continue;
                    }
                    String key = line.substring(0, tab);
                    long value = Long.parseLong(line.substring(tab + 1));
                    Long prior = counts.get(key);
                    counts.put(key, prior == null ? value : prior + value);
                }
            }
        }
        return counts;
    }

    private static String[] window(Map<String, Long> counts, String jobName) {
        String scope = "demand_hotspots".equals(jobName)
                || "cancellation_analysis".equals(jobName) ? "ride" : "all";
        String start = null;
        String end = null;
        for (String key : counts.keySet()) {
            String[] parts = ResultWriter.split(key);
            if (!"W".equals(parts[0]) || !scope.equals(parts[1])) {
                continue;
            }
            String date = parts[2];
            if (start == null || date.compareTo(start) < 0) {
                start = date;
            }
            if (end == null || date.compareTo(end) > 0) {
                end = date;
            }
        }
        return new String[] {start, end};
    }

    static Map<String, String> flags(String[] args) {
        Map<String, String> out = new HashMap<String, String>();
        for (String arg : args) {
            if (!arg.startsWith("--") || !arg.contains("=")) {
                throw new IllegalArgumentException("bad argument " + arg);
            }
            int cut = arg.indexOf('=');
            out.put(arg.substring(2, cut), arg.substring(cut + 1));
        }
        return out;
    }

    private static String required(Map<String, String> flags, String name) {
        String value = flags.get(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("missing --" + name);
        }
        return value;
    }
}
