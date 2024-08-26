package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.primitive.DBSPTypeInteger;

import javax.annotation.Nullable;
import java.math.BigInteger;

/** Base class for all integer literals. */
public abstract class DBSPIntLiteral extends DBSPLiteral {
    protected DBSPIntLiteral(CalciteObject node, DBSPType type, boolean isNull) {
        super(node, type, isNull);
    }

    public DBSPTypeInteger getIntegerType() {
        return this.type.to(DBSPTypeInteger.class);
    }

    @Nullable
    public abstract BigInteger getValue();
}
