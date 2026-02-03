package org.dbsp.sqlCompiler.ir.type.user;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.RESULT;

/** Represents the type SqlResult[T] from sqllib as a TypeUser. */
public class DBSPTypeSqlResult extends DBSPTypeUser {
    public DBSPTypeSqlResult(DBSPType resultType) {
        super(resultType.getNode(), RESULT, "SqlResult", false, resultType);
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

    public DBSPType getWrappedType() {
        return this.getTypeArg(0);
    }

    // sameType and hashCode inherited from TypeUser.
}
