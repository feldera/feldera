package org.dbsp.sqlCompiler.ir.type.user;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import java.lang.module.FindException;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.RESULT;
import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.USER;

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
}
