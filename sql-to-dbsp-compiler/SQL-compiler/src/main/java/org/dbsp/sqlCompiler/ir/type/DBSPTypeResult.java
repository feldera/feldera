package org.dbsp.sqlCompiler.ir.type;

import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeStr;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.RESULT;

/** Represents the type of a Rust Result[T, 'str] type as a TypeUser. */
@NonCoreIR
public class DBSPTypeResult extends DBSPTypeUser {
    public DBSPTypeResult(DBSPType resultType) {
        super(resultType.getNode(), RESULT, "Result", false,
                resultType, new DBSPTypeStr(CalciteObject.EMPTY, false));
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

    // sameType and hashCode inherited from TypeUser.
}
