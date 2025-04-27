package org.dbsp.sqlCompiler.ir.type.user;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.derived.DBSPTypeTupleBase;
import org.dbsp.util.Utilities;

import java.util.List;

/** Represents to the Rust type WithCustomOrd, which is used to wrap
 * values together with a comparator. */
public class DBSPTypeWithCustomOrd extends DBSPTypeUser {
    public DBSPTypeWithCustomOrd(CalciteObject node, DBSPType dataType, DBSPType comparatorType) {
        super(node, DBSPTypeCode.USER, "WithCustomOrd", false,
                dataType, comparatorType);
        Utilities.enforce(dataType.is(DBSPTypeTupleBase.class));
    }

    /** The type of the data that is wrapped.  Always a tuple type */
    public DBSPTypeTupleBase getDataType() {
        return this.typeArgs[0].to(DBSPTypeTupleBase.class);
    }

    /** The type of the data that is wrapped.  Always a tuple type */
    public DBSPComparatorType getComparatorType() {
        return this.typeArgs[1].to(DBSPComparatorType.class);
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
        if (mayBeNull)
            throw new UnsupportedException(this.getNode());
        return this;
    }

    public int size() {
        return this.getDataType().size();
    }

    @SuppressWarnings("unused")
    public static DBSPTypeWithCustomOrd fromJson(JsonNode node, JsonDecoder decoder) {
        List<DBSPType> typeArgs = fromJsonInnerList(node, "typeArgs", decoder, DBSPType.class);
        Utilities.enforce(typeArgs.size() == 2);
        boolean mayBeNull = fromJsonMayBeNull(node);
        return new DBSPTypeWithCustomOrd(CalciteObject.EMPTY, typeArgs.get(0), typeArgs.get(1));
    }
}
