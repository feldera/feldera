package org.dbsp.sqlCompiler.ir.type.user;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

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
        for (DBSPType type: this.typeArgs)
            type.accept(visitor);
        visitor.pop(this);
        visitor.postorder(this);
    }

    // sameType, visit, and hashCode inherited from TypeUser.
}
