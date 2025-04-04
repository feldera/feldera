package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;

public class Print extends CircuitVisitor {
    private final String message;

    public Print(DBSPCompiler compiler, String message) {
        super(compiler);
        this.message = message;
    }

    @Override
    public Token startVisit(IDBSPOuterNode node) {
        System.out.println(this.message);
        return super.startVisit(node);
    }

    @Override
    public VisitDecision preorder(IDBSPOuterNode node) {
        return VisitDecision.STOP;
    }
}
