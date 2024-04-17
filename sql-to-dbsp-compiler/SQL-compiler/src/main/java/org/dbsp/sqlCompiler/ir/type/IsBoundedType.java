package org.dbsp.sqlCompiler.ir.type;

import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;

/** A type which has minimum and maximum values */
public interface IsBoundedType {
    DBSPLiteral getMaxValue();
    DBSPLiteral getMinValue();
}
