package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;

public class MonotoneClosure extends MonotoneScalar {
    final MonotoneValue body;

    public MonotoneClosure(DBSPClosureExpression expression, MonotoneValue body) {
        super(expression);
        this.body = body;
    }

    @Override
    public ValueProjection getProjection() {
        return this.body.getProjection();
    }
}
