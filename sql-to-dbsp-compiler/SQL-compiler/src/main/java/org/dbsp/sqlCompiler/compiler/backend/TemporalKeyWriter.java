package org.dbsp.sqlCompiler.compiler.backend;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.ir.expression.DBSPForExpression;

/** Temporal comparisons inspect loop bodies without first reducing the keys. */
final class TemporalKeyWriter extends OptimizerExpressionWriter {
    TemporalKeyWriter(DBSPCompiler compiler) {
        super(compiler);
        this.includeShuffleIdentities();
    }

    @Override
    public void postorder(DBSPForExpression loop) {
        this.stream.label("variable").append(loop.variable);
        super.postorder(loop);
    }
}
