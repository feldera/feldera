package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.backend.rust.SourcePositionResource;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPHandleErrorExpression;

/** Visitor which collects all the source position objects from {@link DBSPHandleErrorExpression} expressions */
public class CollectSourcePositions extends InnerVisitor {
    final SourcePositionResource sourcePositionResource;

    public CollectSourcePositions(DBSPCompiler compiler, SourcePositionResource sourcePositionResource) {
        super(compiler);
        this.sourcePositionResource = sourcePositionResource;
    }

    @Override
    public void postorder(DBSPHandleErrorExpression expression) {
        this.sourcePositionResource.allocateKey(this.operatorContext, expression);
    }
}
