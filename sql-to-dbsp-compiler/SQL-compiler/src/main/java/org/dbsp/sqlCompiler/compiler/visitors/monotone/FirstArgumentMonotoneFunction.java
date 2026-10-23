package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;

import javax.annotation.Nullable;

/** A function monotone in its first argument when the others are constant, e.g. ROUND(x, digits)
 * or the start of a TUMBLE window. */
class FirstArgumentMonotoneFunction extends MonotoneFunction {
    FirstArgumentMonotoneFunction(String prefix, int... arities) {
        super(prefix, arities);
    }

    @Override
    @Nullable
    DBSPExpression waterline(MonotoneFunctionCall call) {
        Argument first = call.argument(0);
        if (!first.monotone() || !call.otherArgumentsAreConstant(0))
            return null;
        return call.withWaterline(0, first.waterline());
    }
}
