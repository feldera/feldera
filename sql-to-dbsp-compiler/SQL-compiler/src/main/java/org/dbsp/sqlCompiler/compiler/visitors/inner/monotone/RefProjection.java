package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

public class RefProjection extends ValueProjection {
    public final ValueProjection source;

    public RefProjection(ValueProjection source) {
        super(source.getType().ref());
        this.source = source;
    }

    @Override
    public boolean isEmpty() {
        return this.source.isEmpty();
    }

    @Override
    public DBSPType getProjectionResultType() {
        return this.source.getProjectionResultType().ref();
    }

    @Override
    public MonotoneValue createInput(DBSPExpression projected) {
        return new MonotoneRef(this.source.createInput(projected));
    }

    @Override
    public DBSPExpression project(DBSPExpression expression) {
        return this.source.project(expression).borrow();
    }
}
