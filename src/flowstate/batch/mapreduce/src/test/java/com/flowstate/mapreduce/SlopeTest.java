package com.flowstate.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class SlopeTest {
    @Test
    public void matchesThePythonFixture() {
        Slope fit = Slope.fit(new double[][] {
            {10, 1, 1},
            {12, 2, 1},
            {9, 1, 0},
            {15, 3, 0},
            {11, 2, 1},
            {8, 1, 0},
        });
        assertEquals(6, fit.n);
        assertEquals(2.9000000000000057, fit.estimate, 1e-9);
        assertEquals(1.8942061178684164, fit.low, 1e-9);
        assertEquals(3.905793882131595, fit.high, 1e-9);
    }

    @Test
    public void fewerThanFourRowsHasNoEstimate() {
        Slope fit = Slope.fit(new double[][] {{1, 1, 1}});
        assertEquals(1, fit.n);
        assertNull(fit.estimate);
        assertNull(fit.low);
        assertNull(fit.high);
    }
}
