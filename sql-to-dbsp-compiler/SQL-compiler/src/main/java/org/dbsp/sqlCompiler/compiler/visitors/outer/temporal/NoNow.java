package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.compiler.frontend.ExpressionCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPOpcode;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBool;

/**
 * A Boolean expression that does not involve now
 */
record NoNow(DBSPExpression noNow) implements BooleanExpression {
    @Override
    public boolean compatible(BooleanExpression other) {
        return other.is(NoNow.class);
    }

    @Override
    public BooleanExpression combine(BooleanExpression other) {
        return new NoNow(
                ExpressionCompiler.makeBinaryExpression(this.noNow().getNode(),
                        DBSPTypeBool.create(this.isNullable() || other.isNullable()), DBSPOpcode.AND,
                        this.noNow, other.to(NoNow.class).noNow).wrapBoolIfNeeded());
    }

    @Override
    public BooleanExpression seal() {
        return this;
    }

    @Override
    public boolean isNullable() {
        return this.noNow.getType().mayBeNull;
    }
}
