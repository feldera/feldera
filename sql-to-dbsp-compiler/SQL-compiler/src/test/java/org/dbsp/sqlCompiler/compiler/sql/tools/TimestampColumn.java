package org.dbsp.sqlCompiler.compiler.sql.tools;

import org.dbsp.util.Utilities;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.function.Function;

/** A TIMESTAMP column with LATENESS {@code lateness}, a whole number of seconds shorter than 100 days. */
public final class TimestampColumn<R> extends ColumnWithLateness<R, LocalDateTime> {
    final Duration lateness;

    public TimestampColumn(String name, Function<R, LocalDateTime> read, Duration lateness) {
        super(name, read);
        Utilities.enforce(lateness.toNanosPart() == 0 && lateness.toDays() < 100,
                () -> "Unsupported LATENESS " + lateness);
        this.lateness = lateness;
    }

    @Override
    protected LocalDateTime waterline(LocalDateTime largest) {
        return largest.minus(this.lateness);
    }

    @Override
    protected int compare(LocalDateTime left, LocalDateTime right) {
        return left.compareTo(right);
    }

    @Override
    protected String latenessSql() {
        return String.format("LATENESS INTERVAL '%d %02d:%02d:%02d' DAYS TO SECONDS",
                this.lateness.toDays(), this.lateness.toHoursPart(),
                this.lateness.toMinutesPart(), this.lateness.toSecondsPart());
    }

    @Override
    protected String sqlType() {
        return "TIMESTAMP";
    }
}
