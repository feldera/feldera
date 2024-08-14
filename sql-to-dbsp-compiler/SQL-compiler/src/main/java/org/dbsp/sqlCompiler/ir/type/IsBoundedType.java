package org.dbsp.sqlCompiler.ir.type;

import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;

/** A type which has minimum and maximum values */
public interface IsBoundedType {
    DBSPExpression getMaxValue();
    DBSPExpression getMinValue();
}
