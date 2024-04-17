package org.dbsp.sqlCompiler.ir.type;

public interface IsNumericLiteral {
    /** True if the literal is positive */
    boolean gt0();
}
