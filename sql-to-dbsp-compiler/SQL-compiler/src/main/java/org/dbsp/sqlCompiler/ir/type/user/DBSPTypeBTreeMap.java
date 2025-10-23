package org.dbsp.sqlCompiler.ir.type.user;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

import java.util.List;

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

    /** BTreeMap::new() or Some(BTreeMap::new()), depending on nullability */
    public DBSPExpression emptyMap() {
        DBSPExpression map = this.constructor("new");
        if (!this.mayBeNull)
            return map;
        return map.some();
    }

    @Override
    public void accept(InnerVisitor visitor) {
        VisitDecision decision = visitor.preorder(this);
        if (decision.stop()) return;
        visitor.push(this);
        visitor.startArrayProperty("typeArgs");
        int index = 0;
        for (DBSPType type: this.typeArgs) {
            visitor.propertyIndex(index);
            index++;
            type.accept(visitor);
        }
        visitor.endArrayProperty("typeArgs");
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

    @SuppressWarnings("unused")
    public static DBSPTypeBTreeMap fromJson(JsonNode node, JsonDecoder decoder) {
        List<DBSPType> typeArgs = fromJsonInnerList(node, "typeArgs", decoder, DBSPType.class);
        Utilities.enforce(typeArgs.size() == 2);
        boolean mayBeNull = fromJsonMayBeNull(node);
        return new DBSPTypeBTreeMap(typeArgs.get(0), typeArgs.get(1), mayBeNull);
    }
}
