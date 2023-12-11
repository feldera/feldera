package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;

public class MonotoneScalar extends MonotoneValue {
    public final DBSPExpression expression;

    public MonotoneScalar(DBSPExpression expression) {
        super(expression.getType());
        this.expression = expression;
    }

    @Override
    public DBSPExpression getExpression() {
        return this.expression;
    }

    @Override
    public ValueProjection getProjection() {
        return new ScalarProjection(this.expression.getType());
    }

    @Override
    public boolean isEmpty() {
        return false;
    }
}
