package org.dbsp.sqlCompiler.ir.type.user;

import org.dbsp.sqlCompiler.compiler.visitors.VisitDecision;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.ir.NonCoreIR;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.TYPEDBOX;
import static org.dbsp.sqlCompiler.ir.type.DBSPTypeCode.USER;

public class DBSPTypeTypedBox extends DBSPTypeUser {
    static DBSPType[] makeTypeArgs(DBSPType arg, boolean typed) {
        DBSPType[] result = new DBSPType[2];
        result[0] = arg;
        if (typed) {
            result[1] = new DBSPTypeUser(arg.getNode(), USER, "DynDataTyped", false, arg);
        } else {
            result[1] = new DBSPTypeUser(arg.getNode(), USER, "DynData", false);
        }
        return result;
    }

    /** @param typed If true the type is TypedBox<T, DynData>,
     *              else it is TypedBox<T, DynDataTyped<T>> */
    public DBSPTypeTypedBox(DBSPType argType, boolean typed) {
        super(argType.getNode(), TYPEDBOX, "TypedBox", false,
                makeTypeArgs(argType, typed));
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
