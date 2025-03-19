package org.dbsp.sqlCompiler.circuit;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.DBSPNode;
import org.dbsp.sqlCompiler.ir.IDBSPOuterNode;
import org.dbsp.sqlCompiler.ir.statement.DBSPItem;
import org.dbsp.util.IIndentStream;

/** Wraps a declaration (an item) into an OuterNode */
public final class DBSPDeclaration extends DBSPNode implements IDBSPOuterNode {
    public final DBSPItem item;

    public DBSPDeclaration(DBSPItem item) {
        super(item.getNode());
        this.item = item;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        visitor.property("item");
        this.item.accept(visitor);
    }

    @Override
    public long getDerivedFrom() {
        return this.id;
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
    public IIndentStream toString(IIndentStream builder) {
        return this.item.toString(builder);
    }

    @SuppressWarnings("unused")
    public static DBSPDeclaration fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPItem item = fromJsonInner(node, "item", decoder, DBSPItem.class);
        return new DBSPDeclaration(item);
    }
}
