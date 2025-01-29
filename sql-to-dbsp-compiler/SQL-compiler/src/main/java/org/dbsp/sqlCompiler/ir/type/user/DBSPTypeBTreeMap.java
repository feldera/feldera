package org.dbsp.sqlCompiler.ir.type.user;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.USER;

/** Represents the type of a Rust Map as a TypeUser. */
public class DBSPTypeBTreeMap extends DBSPTypeUser {
    public DBSPTypeBTreeMap(DBSPType mapKeyType, DBSPType mapValueType, boolean mayBeNull) {
        super(mapKeyType.getNode(), USER, "BTreeMap", mayBeNull, mapKeyType, mapValueType);
    }

    public DBSPType getKeyType() {
        return this.getTypeArg(0);
    }

    public DBSPType getValueType() {
        return this.getTypeArg(1);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        for (DBSPType type: this.typeArgs)
            type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DBSPTypeBTreeMap(this.getKeyType(), this.getValueType(), mayBeNull);
    }

    // sameType and hashCode inherited from TypeUser.
}
