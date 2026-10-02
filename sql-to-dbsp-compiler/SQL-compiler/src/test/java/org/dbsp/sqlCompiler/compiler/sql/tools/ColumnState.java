package org.dbsp.sqlCompiler.compiler.sql.tools;

import javax.annotation.Nullable;

/** The waterline state of a column with LATENESS.
 * @param <R>  Type of a row.
 * @param <V>  Type of a value of the column. */
final class ColumnState<R, V> {
    final ColumnWithLateness<R, V> column;
    /** Largest value of the column committed so far (commit happens at the end of a step). */
    @Nullable V largest = null;
    /** Largest value of the column in the current step. */
    @Nullable V pending = null;

    ColumnState(ColumnWithLateness<R, V> column) {
        this.column = column;
    }

    /** True if the value of the column in {@code row} is below the waterline of the committed
     * steps; a NULL value, or any value before the first commit, is never late. */
    boolean isLate(R row) {
        V value = this.column.read.apply(row);
        return value != null && this.largest != null &&
                this.column.compare(value, this.column.waterline(this.largest)) < 0;
    }

    /** Record the value of the column in {@code row}, a row inserted or deleted in the current step. */
    void observe(R row) {
        V value = this.column.read.apply(row);
        if (value != null && (this.pending == null || this.column.compare(value, this.pending) > 0))
            this.pending = value;
    }

    /** End the current step: its largest value, if larger, becomes the largest value of the
     * committed steps, which moves the waterline. */
    void commit() {
        if (this.pending != null &&
                (this.largest == null || this.column.compare(this.pending, this.largest) > 0))
            this.largest = this.pending;
        this.pending = null;
    }

    @Override
    public String toString() {
        if (this.largest == null)
            return this.column + ": no waterline";
        return this.column + ": largest " + this.largest + ", waterline " + this.column.waterline(this.largest);
    }
}
