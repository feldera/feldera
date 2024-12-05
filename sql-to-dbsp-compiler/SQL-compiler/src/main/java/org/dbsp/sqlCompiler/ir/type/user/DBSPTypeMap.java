package org.dbsp.sqlCompiler.ir.type.user;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPMapLiteral;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Linq;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.MAP;

/** Represents the type of a Rust Map as a TypeUser. */
public class DBSPTypeMap extends DBSPTypeUser {
    public DBSPTypeMap(DBSPType mapKeyType, DBSPType mapValueType, boolean mayBeNull) {
        super(mapKeyType.getNode(), MAP, "BTreeMap", mayBeNull, mapKeyType, mapValueType);
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
    public boolean hasCopy() {
        return false;
    }

    @Override
    public DBSPType withMayBeNull(boolean mayBeNull) {
        if (mayBeNull == this.mayBeNull)
            return this;
        return new DBSPTypeMap(this.getKeyType(), this.getValueType(), mayBeNull);
    }

    @Override
    public DBSPExpression defaultValue() {
        if (this.mayBeNull)
            return this.none();
        return new DBSPMapLiteral(this, Linq.list());
    }

    // sameType and hashCode inherited from TypeUser.
}
