package org.dbsp.sqlCompiler.compiler.visitors.inner.monotone;

import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;

public class MonotoneConstant extends MonotoneScalar {
    public MonotoneConstant(DBSPLiteral expression) {
        super(expression);
    }
}
