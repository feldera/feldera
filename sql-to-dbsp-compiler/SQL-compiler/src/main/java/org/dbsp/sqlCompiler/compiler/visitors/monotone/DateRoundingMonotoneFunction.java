package org.dbsp.sqlCompiler.compiler.visitors.monotone;

/** A function that rounds a DATE or a TIMESTAMP to a unit, e.g. FLOOR(ts TO DECADE).  It fails
 * when the result is outside the years 1 to 9999. */
final class DateRoundingMonotoneFunction extends UnaryMonotoneFunction {
    DateRoundingMonotoneFunction(String prefix) {
        super(prefix);
    }

    @Override
    boolean canFail() {
        return true;
    }
}
