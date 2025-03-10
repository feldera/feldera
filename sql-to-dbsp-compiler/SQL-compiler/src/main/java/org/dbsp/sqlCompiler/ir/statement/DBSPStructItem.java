package org.dbsp.sqlCompiler.ir.statement;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.EquivalenceContext;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeStruct;
import org.dbsp.sqlCompiler.ir.type.IHasType;
import org.dbsp.util.IIndentStream;

/** An item that declares a struct. */
public final class DBSPStructItem extends DBSPItem implements IHasType {
    public final DBSPTypeStruct type;

    public DBSPStructItem(DBSPTypeStruct type) {
        this.type = type;
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.property("type");
        this.type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPType getType() {
        return this.type;
    }

    @Override
    public boolean sameFields(IDBSPInnerNode other) {
        DBSPStructItem o = other.as(DBSPStructItem.class);
        if (o == null)
            return false;
        return this.type.sameType(o.type);
    }

    @Override
    public IIndentStream toString(IIndentStream builder) {
        return builder.append(this.type);
    }

    @Override
    public DBSPStatement deepCopy() {
        return new DBSPStructItem(this.type);
    }

    @Override
    public EquivalenceResult equivalent(EquivalenceContext context, DBSPStatement other) {
        // Two different struct items are never equivalent
        return new EquivalenceResult(false, context);
    }

    @SuppressWarnings("unused")
    public static DBSPStructItem fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPTypeStruct type = fromJsonInner(node, "type", decoder, DBSPTypeStruct.class);
        return new DBSPStructItem(type);
    }

    @Override
    public String getName() {
        return this.type.name.name();
    }
}
