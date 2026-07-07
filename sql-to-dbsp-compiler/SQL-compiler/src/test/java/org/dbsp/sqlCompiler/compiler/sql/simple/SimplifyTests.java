package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Tests for constant folds performed by the Simplify visitor. */
public class SimplifyTests extends SqlIoTest {
    @Test
    public void modByOneNullable() {
        // NULL % 1 is NULL, so the fold of x % 1 to the constant 0
        // requires a non-nullable left operand
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT);
                CREATE VIEW V AS SELECT x % 1 AS m FROM T;""");
        ccs.stepWeightOne("INSERT INTO T VALUES(NULL);", """
                 m
                ---
                NULL""");
        ccs.stepWeightOne("INSERT INTO T VALUES(5);", """
                 m
                ---
                 0""");
    }

    @Test
    public void mulByZeroInfinity() {
        // 0e0 * Infinity is NaN under IEEE semantics, so the fold of
        // 0e0 * x to the constant 0 requires an exact numeric type
        var ccs = this.getCCS("""
                CREATE TABLE T(x DOUBLE NOT NULL);
                CREATE VIEW V AS SELECT CAST(0.0e0 * (x + x) AS VARCHAR) AS p FROM T;""");
        // x + x overflows to Infinity
        ccs.stepWeightOne("INSERT INTO T VALUES(1e308);", """
                 p
                -----
                 NaN""");
    }
}
