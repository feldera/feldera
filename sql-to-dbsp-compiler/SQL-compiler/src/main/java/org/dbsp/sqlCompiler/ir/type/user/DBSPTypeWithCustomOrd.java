package org.dbsp.sqlCompiler.ir.type.user;

import org.dbsp.sqlCompiler.compiler.errors.UnsupportedException;
import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeAny;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeCode;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTupleBase;

/** Maps to the Rust type WithCustomOrd, which is used to wrap
 * values together with a comparator. */
public class DBSPTypeWithCustomOrd extends DBSPTypeUser {
    public DBSPTypeWithCustomOrd(CalciteObject node, DBSPType dataType) {
        super(node, DBSPTypeCode.USER, "WithCustomOrd", false,
                dataType, DBSPTypeAny.getDefault());
        assert dataType.is(DBSPTypeTupleBase.class);
    }

    @Override
    public boolean hasCopy() {
        return false;
    }

    /** The type of the data that is wrapped.  Always a tuple type */
    public DBSPTypeTupleBase getDataType() {
        return this.typeArgs[0].to(DBSPTypeTupleBase.class);
    }

    @Override
    public DBSPType setMayBeNull(boolean mayBeNull) {
        if (mayBeNull)
            throw new UnsupportedException(this.getNode());
        return this;
    }

    public int size() {
        return this.getDataType().size();
    }
}
