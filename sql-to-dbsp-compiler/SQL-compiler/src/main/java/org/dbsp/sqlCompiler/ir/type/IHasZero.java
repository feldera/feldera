package org.dbsp.sqlCompiler.ir.type;

import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;

/** A type that has a Zero value */
public interface IHasZero {
    DBSPLiteral getZero();
    default boolean isZero(DBSPLiteral literal) {
        return this.getZero().sameValue(literal);
    }
}
