package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeBaseType;

/** The projection of a scalar value */
public class ScalarProjection extends ValueProjection {
    public ScalarProjection(DBSPType type) {
        super(type);
        assert type.is(DBSPTypeBaseType.class):
                "Expected a base type: " + type;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public DBSPType getProjectionResultType() {
        return this.type;
    }

    @Override
    public MonotoneValue createInput(DBSPExpression expression) {
        assert expression.getType().sameType(this.type):
            "Types differ " + expression.getType() + " vs " + this.type;
        return new MonotoneScalar(expression);
    }

    public DBSPExpression project(DBSPExpression expression) {
        return expression;
    }
}
