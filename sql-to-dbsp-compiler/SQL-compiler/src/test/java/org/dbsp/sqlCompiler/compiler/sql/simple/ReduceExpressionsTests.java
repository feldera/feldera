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

    @Test
    public void datetimeIntervalConstants() {
        // The folded constants have the values that the runtime computes
        this.qst("""
                SELECT DATE '2020-01-31' + INTERVAL '1' MONTH;
                 r
                ---
                 2020-02-29
                (1 row)

                SELECT DATE '2020-03-31' - INTERVAL '1' MONTH;
                 r
                ---
                 2020-02-29
                (1 row)

                SELECT DATE '2020-03-01' - INTERVAL '25' HOUR;
                 r
                ---
                 2020-02-29
                (1 row)

                SELECT INTERVAL '1' YEAR + DATE '2019-01-01';
                 r
                ---
                 2020-01-01
                (1 row)

                SELECT TIMESTAMP '2020-01-31 10:00:00.123456' + INTERVAL '1' MONTH;
                 r
                ---
                 2020-02-29 10:00:00.123456
                (1 row)

                SELECT TIMESTAMP '2020-03-01 00:00:00' - INTERVAL '1.5' SECOND;
                 r
                ---
                 2020-02-29 23:59:58.500000
                (1 row)

                SELECT DATE '2020-03-01' - INTERVAL '1' HOUR;
                 r
                ---
                 2020-03-01
                (1 row)

                SELECT DATE '2020-03-01' + INTERVAL '-25' HOUR;
                 r
                ---
                 2020-02-29
                (1 row)

                SELECT TIMESTAMP '2020-02-28 23:00:00.000000' + INTERVAL '1 02:03:04.5' DAY TO SECOND;
                 r
                ---
                 2020-03-01 01:03:04.500000
                (1 row)""");
    }

    @Test
    public void dateOutOfRange() {
        // The compiler does not fold a date after 9999-12-31; the runtime reports the error
        this.runtimeConstantFail("SELECT DATE '9999-12-31' + INTERVAL '1' DAY", "Cannot add interval");
    }

    @Test
    public void nestedCastToVarchar() {
        // The normalizing RexSimplify must keep the inner cast; the text depends on its type (CALCITE-7870)
        var ccs = this.getCCS("""
                CREATE TABLE t(i INT);
                CREATE VIEW v AS SELECT CAST(CAST(i AS DOUBLE) AS VARCHAR) AS d FROM t;""")
                .withStringTrim();
        ccs.step("INSERT INTO t VALUES (-12);", """
                 d     | weight
                ----------------
                 -12.0 | 1""");
    }

    @Test
    public void datetimeIntervalFilter() {
        var ccs = this.getCCS("""
                CREATE TABLE t(d DATE, i INT);
                CREATE VIEW v AS SELECT i FROM t WHERE d = DATE '2020-01-31' + INTERVAL '1' MONTH;""");
        ccs.step("INSERT INTO t VALUES ('2020-02-29', 1), ('2020-03-02', 2);", """
                 i | weight
                ------------
                 1 | 1""");
    }
}
