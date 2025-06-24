package org.dbsp.sqlCompiler.ir.statement;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.expression.DBSPStaticExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.IIndentStream;

/* An item that hold a declaration of a constant value that will be implemented as a LazyLock value in Rust. */
public class DBSPStaticItem extends DBSPItem {
    public final DBSPStaticExpression expression;

    public DBSPStaticItem(DBSPStaticExpression expression) {
        super(expression.getNode());
        this.expression = expression;
    }

    @Override
    public String getName() {
        return this.expression.getName();
    }

    @Override
    public DBSPStatement deepCopy() {
        return new DBSPStaticItem(this.expression);
    }

    @Override
    public EquivalenceResult equivalent(EquivalenceContext context, DBSPStatement other) {
        return new EquivalenceResult(false, context);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("expression");
        this.expression.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPStaticItem o = other.as(DBSPStaticItem.class);
        if (o == null)
            return false;
        return this.expression == o.expression;
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.expression.getName());
    }

    @Override
    public DBSPType getType() {
        return this.expression.getType();
    }

    @SuppressWarnings("unused")
    public static DBSPStaticItem fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPStaticExpression expression = fromJsonInner(node, "expression", decoder, DBSPStaticExpression.class);
        return new DBSPStaticItem(expression);
    }
}
