package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;

/**
 * A Boolean expression that may involve now(), but is not a temporal filter
 */
record NonTemporalFilter(DBSPExpression expression) implements BooleanExpression {
    @Override
    public boolean compatible(BooleanExpression other) {
        // This is compatible with anything else
        return true;
    }

    @Override
    public BooleanExpression combine(BooleanExpression other) {
        DBSPExpression otherExpression;
        if (other.is(NoNow.class))
            otherExpression = other.to(NoNow.class).noNow();
        else if (other.is(NonTemporalFilter.class))
            otherExpression = other.to(NonTemporalFilter.class).expression;
        else
            throw new InternalCompilerError("Unexpected temporal filter " + other);
        return new NonTemporalFilter(
                ExpressionCompiler.makeBinaryExpression(this.expression().getNode(),
                        DBSPTypeBool.create(this.isNullable() || other.isNullable()), DBSPOpcode.AND,
                        this.expression, otherExpression).wrapBoolIfNeeded());
    }

    @Override
    public BooleanExpression seal() {
        return this;
    }

    @Override
    public boolean isNullable() {
        return this.expression.getType().mayBeNull;
    }
}
