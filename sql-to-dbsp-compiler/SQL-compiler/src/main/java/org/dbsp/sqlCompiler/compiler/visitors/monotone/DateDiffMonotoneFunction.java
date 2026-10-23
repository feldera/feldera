package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;

import javax.annotation.Nullable;

/** datediff_(left, right), which computes right - left; monotone in right for a constant left. */
final class DateDiffMonotoneFunction extends MonotoneFunction {
    DateDiffMonotoneFunction() {
        super("datediff_", 2);
    }

    @Override
    @Nullable
    DBSPExpression waterline(MonotoneFunctionCall call) {
        Argument right = call.argument(1);
        if (!right.monotone() || !call.otherArgumentsAreConstant(1))
            return null;
        return call.withWaterline(1, right.waterline());
    }
}
