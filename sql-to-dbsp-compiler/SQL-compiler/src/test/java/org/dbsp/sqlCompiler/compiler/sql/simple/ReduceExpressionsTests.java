package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Tests for the constant folding performed by ReduceExpressionsRule. */
public class ReduceExpressionsTests extends SqlIoTest {
    @Test
    public void filterConstantMatchesProjection() {
        var ccs = this.getCCS("""
                CREATE TABLE t(d DECIMAL(5, 2), i INT);
                CREATE VIEW v AS SELECT i, CAST('1.239' AS DECIMAL(5, 2)) AS c
                FROM t WHERE d = CAST('1.239' AS DECIMAL(5, 2));""");
        ccs.step("INSERT INTO t VALUES (1.23, 1), (1.24, 2);", """
                 i | c    | weight
                -------------------
                 2 | 1.24 | 1""");
    }
}
