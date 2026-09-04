package com.flowstate.mapreduce;

/** Scaffold: YARN submit. */
public class DemandCountDriver {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: DemandCountDriver <input> <output>");
            System.exit(1);
        }
        throw new UnsupportedOperationException("TODO: configure Job and submit to YARN");
    }
}
