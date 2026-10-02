package org.dbsp.sqlCompiler.compiler.sql.tools;

import java.util.function.Function;

/** Models a column with LATENESS: its name, how to read its value from a row, how values
 * compare, and the waterline they imply.
 * @param <R>  Type of a row.
 * @param <V>  Type of a value of the column. */
public abstract class ColumnWithLateness<R, V> {
    /** Name of the column. */
    final String name;
    /** Reads the value of the column from a row; returns null for NULL. */
    final Function<R, V> read;

    /** @param name  Name of the column.
     * @param read  Reads the value of the column from a row; returns null for NULL. */
    protected ColumnWithLateness(String name, Function<R, V> read) {
        this.name = name;
        this.read = read;
    }

    /** The waterline of the column when {@code largest} is its largest value. */
    protected abstract V waterline(V largest);

    /** Negative, zero, or positive when {@code left} is below, equal to, or above {@code right}. */
    protected abstract int compare(V left, V right);

    /** The LATENESS clause of the column in a CREATE TABLE statement. */
    protected abstract String latenessSql();

    @Override
    public String toString() {
        return this.name;
    }
}
