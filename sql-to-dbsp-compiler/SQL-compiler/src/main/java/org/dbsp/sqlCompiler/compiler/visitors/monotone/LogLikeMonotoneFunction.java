package org.dbsp.sqlCompiler.compiler.visitors.monotone;

/** A function of one argument monotone for a non-negative argument, e.g. LN.  LN and LOG10 fail
 * below 0. */
final class LogLikeMonotoneFunction extends UnaryMonotoneFunction {
    LogLikeMonotoneFunction(String prefix) {
        super(prefix);
    }

    @Override
    boolean canFail() {
        return true;
    }
}
