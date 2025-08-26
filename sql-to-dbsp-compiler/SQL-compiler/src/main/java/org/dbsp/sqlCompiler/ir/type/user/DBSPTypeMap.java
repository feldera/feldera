package org.dbsp.sqlCompiler.ir.type.user;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPMapExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.ICollectionType;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeRawTuple;
import org.dbsp.util.Linq;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.List;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.MAP;

/** Represents the type of a Rust Map as a TypeUser.
 * As collection, it contains key-value pair tuples. */
public class DBSPTypeMap extends DBSPTypeUser implements ICollectionType {
    public final DBSPTypeRawTuple collectionElementType;

    public DBSPTypeMap(DBSPType mapKeyType, DBSPType mapValueType, boolean mayBeNull) {
        super(mapKeyType.getNode(), MAP, "Map", mayBeNull, mapKeyType, mapValueType);
        this.collectionElementType = new DBSPTypeRawTuple(this.getKeyType(), this.getValueType());
    }

    @Nullable
    @Override
    public String asSqlString() {
        return "MAP<" + this.getKeyType().asSqlString() + ", " +
                this.getValueType().asSqlString() + ">";
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

    @SuppressWarnings("unused")
    public static DBSPTypeMap fromJson(JsonNode node, JsonDecoder decoder) {
        List<DBSPType> typeArgs = fromJsonInnerList(node, "typeArgs", decoder, DBSPType.class);
        Utilities.enforce(typeArgs.size() == 2);
        boolean mayBeNull = fromJsonMayBeNull(node);
        return new DBSPTypeMap(typeArgs.get(0), typeArgs.get(1), mayBeNull);
    }

    @Override
    public DBSPType getElementType() {
        return this.collectionElementType;
    }
}
