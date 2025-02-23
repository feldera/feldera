package org.dbsp.sqlCompiler.ir.expression.literal;

import org.dbsp.sqlCompiler.compiler.frontend.calciteObject.CalciteObject;
import org.dbsp.sqlCompiler.ir.type.DBSPType;

import javax.annotation.Nullable;

public abstract class DBSPFPLiteral extends DBSPLiteral {
    protected DBSPFPLiteral(CalciteObject node, DBSPType type, @Nullable Object value) {
        super(node, type, value == null);
    }
}
