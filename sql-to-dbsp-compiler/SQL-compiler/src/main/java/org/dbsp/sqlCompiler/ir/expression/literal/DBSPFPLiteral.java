package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;

public abstract class DBSPFPLiteral extends DBSPLiteral {
    /** If true this represents a raw Rust value.
     * Otherwise, it's a SQL FP value. */
    public final boolean raw;

    protected DBSPFPLiteral(CalciteObject node, DBSPType type, @Nullable Object value, boolean raw) {
        super(node, type, value == null);
        this.raw = raw;
    }

    @Override
    public boolean isConstant() {
        return true;
    }
}
