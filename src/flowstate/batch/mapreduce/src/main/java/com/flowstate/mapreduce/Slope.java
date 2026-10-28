package com.flowstate.mapreduce;

/** Weekday-adjusted slope. Same arithmetic as association.py. */
public final class Slope {
    public final int n;
    public final Double estimate;
    public final Double low;
    public final Double high;

    private Slope(int n, Double estimate, Double low, Double high) {
        this.n = n;
        this.estimate = estimate;
        this.low = low;
        this.high = high;
    }

    /** Each row is y, x, weekday (1 or 0). Fewer than four rows has no estimate. */
    public static Slope fit(double[][] rows) {
        int n = rows.length;
        if (n < 4) {
            return new Slope(n, null, null, null);
        }
        double[][] xtx = new double[3][3];
        double[] xty = new double[3];
        for (double[] row : rows) {
            double y = row[0];
            double[] design = {1.0, row[1], row[2]};
            for (int i = 0; i < 3; i++) {
                xty[i] += design[i] * y;
                for (int j = 0; j < 3; j++) {
                    xtx[i][j] += design[i] * design[j];
                }
            }
        }
        double[] beta = solve(xtx, xty);
        if (beta == null) {
            return new Slope(n, null, null, null);
        }
        double residual = 0.0;
        for (double[] row : rows) {
            double fitted = beta[0] + beta[1] * row[1] + beta[2] * row[2];
            double gap = row[0] - fitted;
            residual += gap * gap;
        }
        double sigma2 = residual / (n - 3);
        double[][] inverse = invert(xtx);
        if (inverse == null) {
            return new Slope(n, null, null, null);
        }
        double se = Math.sqrt(Math.max(sigma2 * inverse[1][1], 0.0));
        double estimate = beta[1];
        return new Slope(n, estimate, estimate - 1.96 * se, estimate + 1.96 * se);
    }

    private static double[] solve(double[][] matrix, double[] target) {
        int size = target.length;
        double[][] aug = new double[size][size + 1];
        for (int i = 0; i < size; i++) {
            System.arraycopy(matrix[i], 0, aug[i], 0, size);
            aug[i][size] = target[i];
        }
        for (int col = 0; col < size; col++) {
            int pivot = col;
            for (int row = col + 1; row < size; row++) {
                if (Math.abs(aug[row][col]) > Math.abs(aug[pivot][col])) {
                    pivot = row;
                }
            }
            if (Math.abs(aug[pivot][col]) < 1e-12) {
                return null;
            }
            double[] swap = aug[col];
            aug[col] = aug[pivot];
            aug[pivot] = swap;
            double scale = aug[col][col];
            for (int j = col; j < size + 1; j++) {
                aug[col][j] /= scale;
            }
            for (int row = 0; row < size; row++) {
                if (row == col) {
                    continue;
                }
                double factor = aug[row][col];
                for (int j = col; j < size + 1; j++) {
                    aug[row][j] -= factor * aug[col][j];
                }
            }
        }
        double[] out = new double[size];
        for (int i = 0; i < size; i++) {
            out[i] = aug[i][size];
        }
        return out;
    }

    private static double[][] invert(double[][] matrix) {
        int size = matrix.length;
        double[][] out = new double[size][size];
        for (int col = 0; col < size; col++) {
            double[] target = new double[size];
            target[col] = 1.0;
            double[] solved = solve(copy(matrix), target);
            if (solved == null) {
                return null;
            }
            for (int row = 0; row < size; row++) {
                out[row][col] = solved[row];
            }
        }
        return out;
    }

    private static double[][] copy(double[][] matrix) {
        double[][] out = new double[matrix.length][matrix.length];
        for (int i = 0; i < matrix.length; i++) {
            System.arraycopy(matrix[i], 0, out[i], 0, matrix.length);
        }
        return out;
    }
}
