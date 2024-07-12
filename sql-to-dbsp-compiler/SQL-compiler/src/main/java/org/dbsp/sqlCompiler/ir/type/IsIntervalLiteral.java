package org.dbsp.sqlCompiler.ir.type;

public interface IsIntervalLiteral extends IsNumericLiteral {
    IsIntervalLiteral multiply(long value);
}
