package org.dbsp.sqlCompiler.ir.type.user;

import com.fasterxml.jackson.databind.JsonNode;
import org.dbsp.sqlCompiler.compiler.backend.JsonDecoder;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.util.Utilities;

import java.util.List;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.OPTION;

/** Represents the type of a Rust Option[T] type as a TypeUser. */
@NonCoreIR
public class DBSPTypeOption extends DBSPTypeUser {
    public DBSPTypeOption(DBSPType resultType) {
        super(resultType.getNode(), OPTION, "Option", false, resultType);
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
            type.accept(visitor);
            index++;
        }
        visitor.endArrayProperty("typeArgs");
        visitor.pop(this);
        visitor.postorder(this);
    }

    // sameType, visit, and hashCode inherited from TypeUser.

    @SuppressWarnings("unused")
    public static DBSPTypeOption fromJson(JsonNode node, JsonDecoder decoder) {
        List<DBSPType> typeArgs = fromJsonInnerList(node, "typeArgs", decoder, DBSPType.class);
        Utilities.enforce(typeArgs.size() == 1);
        return new DBSPTypeOption(typeArgs.get(0));
    }
}
