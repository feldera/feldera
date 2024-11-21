package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;

import javax.annotation.Nullable;

/** Interface implemented by objects may have watermark information */
public interface IHasWatermark extends IHasCalciteObject {
    @Nullable
    DBSPExpression getWatermark();
}
