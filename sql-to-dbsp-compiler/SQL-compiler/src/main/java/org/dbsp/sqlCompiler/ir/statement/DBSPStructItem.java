package org.dbsp.sqlCompiler.ir.statement;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.TableMetadata;
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

import javax.annotation.Nullable;

/** An item that declares a struct.
 * The Rust representation also includes a bunch of helper functions for de/serializing
 * struct values. */
@NonCoreIR
public final class DBSPStructItem extends DBSPItem implements IHasType {
    public final DBSPTypeStruct type;
    /** Non-null if this struct represents the schema from a table declaration */
    @Nullable
    public final TableMetadata metadata;

    public DBSPStructItem(DBSPTypeStruct type, @Nullable TableMetadata metadata) {
        super(type.getNode());
        this.type = type;
        this.metadata = metadata;
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
        return builder.append("StructWithHelper ").append(this.type);
    }

    @Override
    public DBSPStatement deepCopy() {
        return new DBSPStructItem(this.type, this.metadata);
    }

    @Override
    public EquivalenceResult equivalent(EquivalenceContext context, DBSPStatement other) {
        // Since this is NonCoreIR we leave this for later
        return new EquivalenceResult(false, context);
    }

    @SuppressWarnings("unused")
    public static DBSPStructItem fromJson(JsonNode node, JsonDecoder decoder) {
        DBSPTypeStruct type = fromJsonInner(node, "type", decoder, DBSPTypeStruct.class);
        TableMetadata metadata = null;
        if (node.has("metadata"))
            metadata = TableMetadata.fromJson(node.get("metadata"), decoder);
        return new DBSPStructItem(type, metadata);
    }

    @Override
    public String getName() {
        return this.type.name.name();
    }
}
