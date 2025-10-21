package org.dbsp.sqlCompiler.compiler.visitors.outer.temporal;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPApplyExpression;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeTimestamp;
import org.dbsp.util.Logger;

/** Discovers whether an expression contains a call to the now() function. */
class ContainsNow extends InnerVisitor {
    public boolean found;
    /** If true the 'found' is reset for each invocation. */
    public final boolean perExpression;

    public ContainsNow(DBSPCompiler compiler, boolean perExpression) {
        super(compiler);
        this.found = false;
        this.perExpression = perExpression;
    }

    static boolean isNow(DBSPApplyExpression node) {
        String name = node.getFunctionName();
        return name != null && name.equalsIgnoreCase("now");
    }

    static DBSPTypeTimestamp timestampType() {
        return DBSPTypeTimestamp.INSTANCE;
    }

    @Override
    public VisitDecision preorder(DBSPApplyExpression node) {
        if (isNow(node)) {
            Logger.INSTANCE.belowLevel(this, 1)
                    .append("Found 'now' call")
                    .newline();
            found = true;
            return VisitDecision.STOP;
        }
        return super.preorder(node);
    }

    @Override
    public void startVisit(IDBSPInnerNode node) {
        super.startVisit(node);
        if (this.perExpression) {
            this.found = false;
        }
    }

    public Boolean found() {
        return this.found;
    }
}
