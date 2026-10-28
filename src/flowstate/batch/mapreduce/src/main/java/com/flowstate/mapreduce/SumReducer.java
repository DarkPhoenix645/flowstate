package com.flowstate.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** Sum partial counts. The key already names the grain. */
public class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private final LongWritable out = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0L;
        for (LongWritable value : values) {
            sum += value.get();
        }
        out.set(sum);
        context.write(key, out);
    }
}
