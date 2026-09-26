package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/** The columns with LATENESS of a table.
 * @param <R>  Type of a row. */
public final class LatenessSchema<R> {
    final List<ColumnWithLateness<R, ?>> columns = new ArrayList<>();
    /** True once a model uses the schema; a sealed schema accepts no more columns. */
    boolean sealed = false;

    /** Add a column with LATENESS; its name must differ from the names of the other columns. */
    public void addColumn(ColumnWithLateness<R, ?> column) {
        Utilities.enforce(!this.sealed, () -> "Column " + column + " added to a sealed schema");
        Utilities.enforce(!this.hasLateness(column.name), () -> "Duplicate column " + column);
        this.columns.add(column);
    }

    /** The column named {@code name}, or null if that column has no LATENESS. */
    @Nullable
    ColumnWithLateness<R, ?> find(String name) {
        for (ColumnWithLateness<R, ?> column : this.columns)
            if (column.name.equals(name))
                return column;
        return null;
    }

    /** True if the column named {@code name} has LATENESS. */
    public boolean hasLateness(String name) {
        return this.find(name) != null;
    }

    /** The LATENESS clause of the column named {@code name} in a CREATE TABLE statement,
     * preceded by a space, or an empty string if the column has no LATENESS. */
    public String lateness(String name) {
        ColumnWithLateness<R, ?> column = this.find(name);
        return column == null ? "" : " " + column.latenessSql();
    }

    /** Accept no more columns. */
    void seal() {
        this.sealed = true;
    }

    /** A new state, with no waterline, for each column; seals the schema. */
    List<ColumnState<R, ?>> newStates() {
        this.seal();
        List<ColumnState<R, ?>> result = new ArrayList<>();
        for (ColumnWithLateness<R, ?> column : this.columns)
            result.add(newState(column));
        return result;
    }

    static <R, V> ColumnState<R, V> newState(ColumnWithLateness<R, V> column) {
        return new ColumnState<>(column);
    }

    @Override
    public String toString() {
        return this.columns.toString();
    }
}
