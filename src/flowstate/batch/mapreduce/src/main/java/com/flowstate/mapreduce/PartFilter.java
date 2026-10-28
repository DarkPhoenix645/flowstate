package com.flowstate.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/** Corpus parts only. _meta and _SUCCESS are not splits. */
public class PartFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
        String name = path.getName();
        // The input directory itself is filtered. Hidden files are not splits.
        return !name.startsWith("_") && !name.startsWith(".");
    }
}
