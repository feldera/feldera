package org.dbsp.sqlCompiler.ir.type.user;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.RESULT;
import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.USER;
import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.util.Utilities;
import java.util.List;

/** Represents the type of a Rust Result[T, Box[dyn Error]] type as a TypeUser. */
@NonCoreIR
public class DBSPTypeResult extends DBSPTypeUser {
    public DBSPTypeResult(DBSPType resultType) {
        super(resultType.getNode(), RESULT, "Result", false,
                resultType,
                new DBSPTypeUser(resultType.getNode(), USER, "Box", false,
                        new DBSPTypeUser(CalciteObject.EMPTY, USER, "dyn std::error::Error", false)));
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

    // sameType and hashCode inherited from TypeUser.

    /** The first type argument is the result type; the second is the boxed error. */
    @SuppressWarnings("unused")
    public static DBSPTypeResult fromJson(JsonNode node, JsonDecoder decoder) {
        List<DBSPType> typeArgs = fromJsonInnerList(node, "typeArgs", decoder, DBSPType.class);
        Utilities.enforce(typeArgs.size() == 2);
        return new DBSPTypeResult(typeArgs.get(0));
    }
}
