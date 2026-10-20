package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.util.Utilities;

import java.time.LocalDate;
import java.util.function.Function;

/** A DATE column with a LATENESS of {@code latenessDays} days, fewer than 100. */
public final class DateColumn<R> extends ColumnWithLateness<R, LocalDate> {
    final int latenessDays;

    public DateColumn(String name, Function<R, LocalDate> read, int latenessDays) {
        super(name, read);
        Utilities.enforce(latenessDays >= 0 && latenessDays < 100,
                () -> "Unsupported LATENESS of " + latenessDays + " days");
        this.latenessDays = latenessDays;
    }

    @Override
    protected LocalDate waterline(LocalDate largest) {
        return largest.minusDays(this.latenessDays);
    }

    @Override
    protected int compare(LocalDate left, LocalDate right) {
        return left.compareTo(right);
    }

    @Override
    protected String latenessSql() {
        return "LATENESS INTERVAL '" + this.latenessDays + "' DAYS";
    }

    @Override
    protected String sqlType() {
        return "DATE";
    }
}
