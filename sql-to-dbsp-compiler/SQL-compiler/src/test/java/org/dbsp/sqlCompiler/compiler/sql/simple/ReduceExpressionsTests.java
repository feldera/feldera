package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer.CalciteOptimizer;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.util.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

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

    @Test
    public void correlatedFilterOverValues() {
        // A filter over VALUES that reads a correlation variable cannot be folded (CALCITE-7450).
        // Folding it to FALSE dropped the VALUES row, and these views failed to compile.
        // Results validated using postgres
        var ccs = this.getCCS("""
                CREATE TABLE t(x INT);
                CREATE VIEW e AS SELECT x FROM t
                WHERE EXISTS (SELECT 1 FROM (VALUES 4) AS d(w) WHERE d.w > t.x);
                CREATE VIEW s AS SELECT x, (SELECT MAX(w) FROM (VALUES 4) AS d(w) WHERE d.w > t.x) AS m FROM t;
                CREATE VIEW l AS SELECT x, z FROM t, LATERAL (
                    SELECT * FROM (VALUES 3) AS c(z)
                    WHERE c.z > t.x AND EXISTS (SELECT 1 FROM (VALUES 4) AS d(w) WHERE d.w > t.x)) AS l;""");
        ccs.stepMultiView("INSERT INTO t VALUES (1), (4), (NULL);", Map.of(
                "e", """
                 x | weight
                ------------
                 1 | 1""",
                "s", """
                 x    | m    | weight
                ---------------------
                 1    | 4    | 1
                 4    | NULL | 1
                 NULL | NULL | 1""",
                "l", """
                 x | z | weight
                ----------------
                 1 | 3 | 1"""));
    }

    @Test
    public void asofJoinConstantFold() {
        // JOIN_REDUCE_EXPRESSIONS must skip an ASOF JOIN: its copy() throws (CALCITE-7273),
        // and the exception made the optimizer skip all constant folding for the view.
        StringBuilder builder = new StringBuilder();
        Appendable save = Logger.INSTANCE.setDebugStream(builder);
        Logger.INSTANCE.setLoggingLevel(CalciteOptimizer.class, 1);
        try {
            // The filter k = 1 lets the rule replace a.k by 1 in the join condition
            var ccs = this.getCCS("""
                    CREATE TABLE t1(k INT, t INT);
                    CREATE TABLE t2(k INT, t INT);
                    CREATE VIEW v AS SELECT a.k, a.t, b.t AS bt, a.t + (1 + 2) AS c
                    FROM (SELECT * FROM t1 WHERE k = 1) a
                    LEFT ASOF JOIN t2 b MATCH_CONDITION (a.t >= b.t) ON a.k = b.k;""");
            String log = builder.toString();
            Assert.assertFalse(log, log.contains("Skipping Constant fold"));
            ccs.step("""
                    INSERT INTO t1 VALUES (1, 10), (2, 10);
                    INSERT INTO t2 VALUES (1, 5), (1, 8), (1, 12);""", """
                     k | t  | bt | c  | weight
                    ---------------------------
                     1 | 10 | 8  | 13 | 1""");
        } finally {
            Logger.INSTANCE.setLoggingLevel(CalciteOptimizer.class, 0);
            Logger.INSTANCE.setDebugStream(save);
        }
    }
}
