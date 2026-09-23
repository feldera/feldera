package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.expression.DBSPCloneExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;

/** Replaces every {@code e.clone()} by {@code e}.  The Rust backend clones a value to satisfy
 * ownership; the Gen-2 engine evaluates expressions over immutable columns, where a clone is
 * the identity. */
public class RemoveClones extends InnerRewriteVisitor {
    public RemoveClones(DBSPCompiler compiler) {
        super(compiler, false);
    }

    @Override
    public VisitDecision preorder(DBSPCloneExpression expression) {
        this.push(expression);
        DBSPExpression cloned = this.transform(expression.expression);
        this.pop(expression);
        this.map(expression, cloned);
        return VisitDecision.STOP;
    }
}
