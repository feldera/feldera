package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;

import javax.annotation.Nullable;

/** A function of one argument, e.g. EXP. */
class UnaryMonotoneFunction extends MonotoneFunction {
    UnaryMonotoneFunction(String names, boolean canFail) {
        super(names, canFail);
    }

    @Override
    @Nullable
    DBSPExpression waterline(MonotoneFunctionCall call) {
        Argument argument = call.argument(0);
        if (!argument.monotone())
            return null;
        return call.withWaterline(0, argument.waterline());
    }
}
