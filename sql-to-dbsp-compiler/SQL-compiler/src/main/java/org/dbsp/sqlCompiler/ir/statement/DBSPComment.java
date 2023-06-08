package org.dbsp.sqlCompiler.ir.statement;

import org.dbsp.sqlCompiler.circuit.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.InnerVisitor;

public class DBSPComment extends DBSPStatement implements IDBSPOuterNode {
    public final String comment;

    public DBSPComment(String comment) {
        super(null);
        this.comment = comment;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        if (!visitor.preorder(this)) return;
        visitor.postorder(this);
    }
}
