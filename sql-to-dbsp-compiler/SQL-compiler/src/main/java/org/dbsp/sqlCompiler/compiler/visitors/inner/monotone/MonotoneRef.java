package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;

public class MonotoneRef extends MonotoneValue {
    public final MonotoneValue source;

    public MonotoneRef(MonotoneValue source) {
        super(source.getType().ref());
        this.source = source;
    }

    @Override
    public DBSPExpression getExpression() {
        return this.source.getExpression().borrow();
    }

    @Override
    public ValueProjection getProjection() {
        return new RefProjection(this.source.getProjection());
    }

    @Override
    public boolean isEmpty() {
        return this.source.isEmpty();
    }
}
