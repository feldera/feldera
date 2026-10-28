package org.dbsp.sqlCompiler.compiler.visitors.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

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

    /** The call of the function that computes the waterline of this call, named by
     * {@link MonotoneFunction#waterlineFunctionName}, with {@code waterline} as argument
     * {@code index} and the waterlines of the constant arguments as the others. */
    DBSPExpression withWaterline(int index, DBSPExpression waterline) {
        DBSPExpression[] result = new DBSPExpression[this.arguments.size()];
        for (int i = 0; i < this.arguments.size(); i++) {
            MonotoneFunction.Argument argument = this.arguments.get(i);
            if (i == index)
                result[i] = waterline;
            else if (argument.monotone())
                result[i] = argument.waterline();
            else
                result[i] = argument.copy();
        }
        String callName = Objects.requireNonNull(this.call.getFunctionName());
        String name = this.function.waterlineFunctionName(callName);
        return new DBSPApplyExpression(this.call.getNode(), name, this.call.getType().withMayBeNull(false), result);
    }

    /** An expression computing the waterline of the call, or null if the call is not monotone. */
    @Nullable
    DBSPExpression waterline() {
        return this.function.waterline(this);
    }
}
