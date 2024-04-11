package org.dbsp.sqlCompiler.ir.expression;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeFunction;

/** Base class for function applications */
public abstract class DBSPApplyBaseExpression extends DBSPExpression {
    public final DBSPExpression function;
    public final DBSPExpression[] arguments;

    void checkArgs(boolean method) {
        DBSPType[] parameterTypes = null;
        if (method) return;  // TODO
        if (this.function.getType().is(DBSPTypeFunction.class)) {
            DBSPTypeFunction funcType = this.function.getType().to(DBSPTypeFunction.class);
            assert funcType.argumentTypes.length == this.arguments.length:
                    "Has " + funcType.argumentTypes.length + " parameters, but only " +
                            this.arguments.length + " arguments";
            parameterTypes = funcType.argumentTypes;
        }
        int index = 0;
        for (DBSPExpression arg: this.arguments) {
            if (arg == null) {
                throw new InternalCompilerError("Null arg", this);
            }
            assert parameterTypes == null || parameterTypes[index].sameType(arg.getType()) :
                    "Argument " + arg + " type " +
                    arg.getType() + " does not match parameter type " + parameterTypes[index];
            index++;
        }
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
