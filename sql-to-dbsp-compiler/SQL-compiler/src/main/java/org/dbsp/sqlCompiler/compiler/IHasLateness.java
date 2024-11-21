package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;

import javax.annotation.Nullable;

/** Interface implemented by objects may have lateness information */
public interface IHasLateness extends IHasCalciteObject {
    @Nullable
    DBSPExpression getLateness();
}
