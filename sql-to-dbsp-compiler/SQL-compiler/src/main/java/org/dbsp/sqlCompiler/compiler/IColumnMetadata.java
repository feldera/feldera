package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.ProgramIdentifier;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.IHasType;

import javax.annotation.Nullable;

public interface IColumnMetadata extends IHasSourcePositionRange, IHasType, IHasCalciteObject {
    @Nullable DBSPExpression getLateness();
    @Nullable DBSPExpression getWatermark();
    @Nullable DBSPExpression getDefaultValue();
    ProgramIdentifier getColumnName();
    boolean isPrimaryKey();
    boolean isInterned();
}
