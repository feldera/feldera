package org.dbsp.sqlCompiler.ir;

public interface IsNumericLiteral extends IDBSPInnerNode {
    /** True if the literal is positive */
    boolean gt0();
    /** Compare the value of with the other literal; must have the same type and neither must be null */
    int compare(IsNumericLiteral other);
    /** Negate the literal value.  May throw if the computation cannot be done. */
    IsNumericLiteral negate();
}
