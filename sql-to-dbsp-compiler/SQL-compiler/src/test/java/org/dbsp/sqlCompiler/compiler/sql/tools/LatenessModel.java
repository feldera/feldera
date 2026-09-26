package org.dbsp.sqlCompiler.compiler.sql.tools;

import java.util.ArrayList;
import java.util.List;

/** Models the LATENESS columns of a table: their waterlines and the rows the table holds.
 * The waterline of a column is computed from the largest value of the column in the earlier
 * steps, so it only moves when a step is committed.  A NULL value is never late.
 *
 * @param <R>  Type of a row. */
public class LatenessModel<R> {
    /** One state per column with LATENESS. */
    final List<ColumnState<R, ?>> states;
    final List<R> contents = new ArrayList<>();

    /** @param schema  The columns with LATENESS. */
    public LatenessModel(LatenessSchema<R> schema) {
        this.states = schema.newStates();
    }

    /** True if some value of {@code row} is below the waterline of its column. */
    public boolean isLate(R row) {
        for (ColumnState<R, ?> state : this.states)
            if (state.isLate(row))
                return true;
        return false;
    }

    /** Insert {@code row} unless it is late.
     * @return True if the row was inserted. */
    public boolean insert(R row) {
        if (this.isLate(row))
            return false;
        this.contents.add(row);
        this.states.forEach(s -> s.observe(row));
        return true;
    }

    /** Delete {@code row} unless it is late or absent.
     * @return True if the row was deleted. */
    public boolean delete(R row) {
        if (this.isLate(row) || !this.contents.remove(row))
            return false;
        this.states.forEach(s -> s.observe(row));
        return true;
    }

    /** End a step: the values of the step move the waterlines of the next steps. */
    public void commit() {
        this.states.forEach(ColumnState::commit);
    }

    @Override
    public String toString() {
        return this.states + " " + this.contents.size() + " rows";
    }
}
