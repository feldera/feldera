package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.util.IHasName;

import javax.annotation.Nullable;

/** Interface implemented by objects may have watermark information */
public interface IHasWatermark extends IHasCalciteObject, IHasName {
    @Nullable
    DBSPExpression getWatermark();
}
