package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;

import javax.annotation.Nullable;
import java.util.List;

/** A call of a monotone function.
 * @param function   The function called.
 * @param call       The call.
 * @param arguments  A description of the arguments. */
record MonotoneFunctionCall(MonotoneFunction function, DBSPApplyExpression call,
                            List<MonotoneFunction.Argument> arguments) {
    /** What the analysis knows about argument {@code index}. */
    MonotoneFunction.Argument argument(int index) {
        return this.arguments.get(index);
    }

    /** True if every argument except argument {@code index} is constant. */
    boolean otherArgumentsAreConstant(int index) {
        for (int i = 0; i < this.arguments.size(); i++)
            if (i != index && !this.arguments.get(i).constant())
                return false;
        return true;
    }

    /** The call with {@code waterline} as argument {@code index}. */
    DBSPExpression withWaterline(int index, DBSPExpression waterline) {
        DBSPExpression[] result = new DBSPExpression[this.arguments.size()];
        for (int i = 0; i < this.arguments.size(); i++)
            result[i] = i == index ? waterline : this.arguments.get(i).copy();
        return this.call.replaceArguments(result);
    }

    /** An expression computing the waterline of the call, or null if the call is not monotone. */
    @Nullable
    DBSPExpression waterline() {
        return this.function.waterline(this);
    }
}
