package org.dbsp.sqlCompiler.ir.type.user;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Linq;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.MAP;

/** Represents the type of a Rust Map as a TypeUser. */
public class DBSPTypeMap extends DBSPTypeUser {
    public DBSPTypeMap(DBSPType mapKeyType, DBSPType mapValueType, boolean mayBeNull) {
        super(mapKeyType.getNode(), MAP, "Map", mayBeNull, mapKeyType, mapValueType);
    }

    public DBSPType getKeyType() {
        return this.getTypeArg(0);
    }

    public DBSPType getValueType() {
        return this.getTypeArg(1);
    }

    @Override
    public DBSPType deref() {
        return this.innerType();
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

    /** Type of the representation of the map; currently Map = Arc[BTreeMap] */
    public DBSPTypeUser innerType() {
        return new DBSPTypeBTreeMap(this.getKeyType(), this.getValueType(), this.mayBeNull);
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
        return new DBSPMapExpression(this, Linq.list());
    }

    // sameType and hashCode inherited from TypeUser.
}
