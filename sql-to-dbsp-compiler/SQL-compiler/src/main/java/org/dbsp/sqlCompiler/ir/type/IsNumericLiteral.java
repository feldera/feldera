package org.dbsp.sqlCompiler.ir.type;

import org.dbsp.sqlCompiler.ir.IDBSPInnerNode;

public interface IsNumericLiteral extends IDBSPInnerNode {
    /** True if the literal is positive */
    boolean gt0();
    /** Negate the literal value.  May throw if the computation cannot be done. */
    IsNumericLiteral negate();
}
