package org.dbsp.sqlCompiler.ir;

import javax.annotation.Nullable;
import java.math.BigInteger;

public interface IsIntervalLiteral extends IsNumericLiteral {
    /** Multiply an interval by an integer value.
     * May throw ArithmeticException on overflow. */
    IsIntervalLiteral multiply(@Nullable BigInteger value);
}
