package org.dbsp.sqlCompiler.compiler.sql.tools;

import java.util.function.Function;

/** An INT column with LATENESS {@code lateness}. */
public final class IntegerColumn<R> extends ColumnWithLateness<R, Integer> {
    final int lateness;

    public IntegerColumn(String name, Function<R, Integer> read, int lateness) {
        super(name, read);
        this.lateness = lateness;
    }

    @Override
    protected Integer waterline(Integer largest) {
        return largest - this.lateness;
    }

    @Override
    protected int compare(Integer left, Integer right) {
        return Integer.compare(left, right);
    }

    @Override
    protected String latenessSql() {
        return "LATENESS " + this.lateness;
    }
}
