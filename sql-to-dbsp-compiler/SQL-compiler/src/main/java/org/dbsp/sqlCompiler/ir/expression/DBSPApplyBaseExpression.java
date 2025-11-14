package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeFunction;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;

/** Base class for function applications */
public abstract class DBSPApplyBaseExpression extends DBSPExpression {
    public final DBSPExpression function;
    public final DBSPExpression[] arguments;

    void checkArgs(boolean method) {
        DBSPType[] parameterTypes = null;
        if (method) return;  // TODO
        if (this.function.getType().is(DBSPTypeFunction.class)) {
            DBSPTypeFunction funcType = this.function.getType().to(DBSPTypeFunction.class);
            Utilities.enforce(funcType.parameterTypes.length == this.arguments.length,
                    () -> "Has " + funcType.parameterTypes.length + " parameters, but " +
                            this.arguments.length + " arguments");
            parameterTypes = funcType.parameterTypes;
        }
        int index = 0;
        for (DBSPExpression arg: this.arguments) {
            if (arg == null) {
                throw new InternalCompilerError("Null argument for apply expression", this);
            }
            if (parameterTypes != null && !parameterTypes[index].sameType(arg.getType()))
                    throw new InternalCompilerError("Argument " + index + " " + arg + " type " +
                    arg.getType() + " does not match parameter type " + parameterTypes[index]);
            index++;
        }
    }

    /** Return the function name if the function is given by name,
     * null otherwise. */
    @Nullable
    public String getFunctionName() {
        DBSPPathExpression pe = this.function.as(DBSPPathExpression.class);
        if (pe == null)
            return null;
        return pe.path.asString();
    }

    protected DBSPApplyBaseExpression(
            CalciteObject node, DBSPExpression function,
            DBSPType returnType, DBSPExpression[] arguments) {
        super(node, returnType);
        this.function = function;
        this.arguments = arguments;
    }

    public static DBSPType getReturnType(DBSPType type) {
        if (type.is(DBSPTypeAny.class))
            return type;
        DBSPTypeFunction func = type.to(DBSPTypeFunction.class);
        return func.resultType;
    }
}
