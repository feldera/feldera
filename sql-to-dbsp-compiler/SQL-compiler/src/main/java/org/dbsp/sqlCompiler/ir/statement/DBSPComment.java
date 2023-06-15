package org.dbsp.sqlCompiler.ir.statement;

import org.dbsp.sqlCompiler.ir.IDBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

public class DBSPComment extends DBSPStatement implements IDBSPOuterNode {
    public final String comment;

    public DBSPComment(String comment) {
        super(null);
        this.comment = comment;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPNode other) {
        DBSPComment o = other.as(DBSPComment.class);
        if (o == null)
            return false;
        return this.comment.equals(o.comment);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        String[] parts = this.comment.split("\n");
        parts = Linq.map(parts, p -> "// " + p, String.class);
        return builder.intercalate("\n", parts);
    }
}
