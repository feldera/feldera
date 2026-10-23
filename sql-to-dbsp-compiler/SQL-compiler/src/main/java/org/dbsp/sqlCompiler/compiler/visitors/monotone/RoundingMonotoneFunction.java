package org.dbsp.sqlCompiler.compiler.visitors.monotone;

/** A function that rounds its first argument to a unit given by its constant arguments, e.g.
 * ROUND(x, digits) of a DECIMAL or the start of a TUMBLE window.  It fails when the result is
 * outside the range of its type. */
final class RoundingMonotoneFunction extends FirstArgumentMonotoneFunction {
    RoundingMonotoneFunction(String prefix, int... arities) {
        super(prefix, arities);
    }

    @Override
    boolean canFail() {
        return true;
    }
}
