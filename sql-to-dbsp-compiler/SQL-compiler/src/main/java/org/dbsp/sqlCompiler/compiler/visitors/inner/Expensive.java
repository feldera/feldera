package org.dbsp.sqlCompiler.compiler.visitors.inner;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyMethodExpression;

/** Visitor which detects whether an expression contains "expensive" subexpressions.
 * Today any external function call is deemed expensive. */
public class Expensive extends InnerVisitor {
    boolean expensive;

    public Expensive(DBSPCompiler compiler) {
        super(compiler);
        this.expensive = false;
    }

    public boolean isExpensive() {
        return this.expensive;
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        super.startVisit(node);
        this.expensive = false;
    }

    @Override
    public VisitDecision preorder(DBSPApplyExpression unused) {
        this.expensive = true;
        return VisitDecision.STOP;
    }

    @Override
    public VisitDecision preorder(DBSPApplyMethodExpression unused) {
        this.expensive = true;
        return VisitDecision.STOP;
    }
}
