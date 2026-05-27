package org.dbsp.sqlCompiler.ir.statement;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

@NonCoreIR
public final class DBSPComment extends DBSPStatement implements IDBSPOuterNode {
    public final String comment;

    public DBSPComment(String comment) {
        super(CalciteObject.EMPTY);
        this.comment = comment;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        visitor.push(this);
        VisitDecision decision = visitor.preorder(this);
        if (!decision.stop())
            visitor.postorder(this);
        visitor.pop(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
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

    @Override
    public DBSPStatement deepCopy() {
        return new DBSPComment(this.comment);
    }

    @Override
    public EquivalenceResult equivalent(EquivalenceContext context, DBSPStatement other) {
        throw new UnsupportedException(this.getNode());
    }

    @SuppressWarnings("unused")
    public static DBSPComment fromJson(JsonNode node, JsonDecoder decoder) {
        String comment = Utilities.getStringProperty(node, "comment");
        return new DBSPComment(comment);
    }
}
