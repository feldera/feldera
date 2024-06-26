package org.dbsp.sqlCompiler.ir.type.user;

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
    public boolean hasCopy() {
        return false;
    }

    // sameType, visit, and hashCode inherited from TypeUser.
}
