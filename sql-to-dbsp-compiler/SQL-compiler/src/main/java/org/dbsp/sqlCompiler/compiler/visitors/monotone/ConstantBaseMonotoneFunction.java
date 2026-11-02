package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPDoubleLiteral;

import javax.annotation.Nullable;

/** A function increasing in one argument when another argument, the base, is a constant above 1,
 * e.g. LOG(x, base) and POWER(base, x). */
final class ConstantBaseMonotoneFunction extends MonotoneFunction {
    /** The index of the argument in which the function is increasing */
    final int monotoneIndex;
    /** The index of the base */
    final int baseIndex;

    ConstantBaseMonotoneFunction(String names, boolean canFail, int monotoneIndex, int baseIndex) {
        super(names, canFail);
        this.monotoneIndex = monotoneIndex;
        this.baseIndex = baseIndex;
    }

    /** True if {@code expression} is a DOUBLE literal above 1. */
    static boolean aboveOne(DBSPExpression expression) {
        DBSPDoubleLiteral literal = expression.as(DBSPDoubleLiteral.class);
        return literal != null && literal.value != null && literal.value > 1.0;
    }

    @Override
    @Nullable
    DBSPExpression waterline(MonotoneFunctionCall call) {
        Argument argument = call.argument(this.monotoneIndex);
        Argument base = call.argument(this.baseIndex);
        if (!argument.monotone() || !base.constant() || !aboveOne(base.expression()))
            return null;
        return call.withWaterline(this.monotoneIndex, argument.waterline());
    }
}
