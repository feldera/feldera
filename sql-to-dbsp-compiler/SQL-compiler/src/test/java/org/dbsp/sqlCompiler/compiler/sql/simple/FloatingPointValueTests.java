package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

import java.util.Map;

/** REAL and DOUBLE values have one zero and one NaN */
public class FloatingPointValueTests extends SqlIoTest {
    @Test
    public void issue7529() {
        // Each view retracts exactly what it inserted
        var ccs = this.getCCS("""
                CREATE TABLE t(x DOUBLE);
                CREATE VIEW str AS SELECT CAST(x AS VARCHAR) AS s FROM t;
                CREATE VIEW neg AS SELECT x FROM t WHERE 1e0 / x < 0e0;
                CREATE VIEW fold AS SELECT CAST(x AS VARCHAR) AS s FROM t WHERE x < 1e0 / -0e0;
                CREATE VIEW ln AS SELECT CAST(LN(x) AS VARCHAR) AS l, CAST(LN(x / x) AS VARCHAR) AS n FROM t;""")
                .withStringTrim();
        String negEmpty = """
                 x | weight
                ------------""";
        String inserted = """
                 s   | weight
                --------------
                 0.0 | 2""";
        ccs.stepMultiView("INSERT INTO t VALUES (CAST('-0' AS DOUBLE)), (0e0);", Map.of(
                "str", inserted,
                "fold", inserted,
                "neg", negEmpty,
                "ln", """
                 l    | n   | weight
                ---------------------
                 -inf | NaN | 2"""));
        for (String value : new String[] { "CAST('-0' AS DOUBLE)", "0e0" }) {
            String deleted = """
                     s   | weight
                    --------------
                     0.0 | -1""";
            ccs.stepMultiView("REMOVE FROM t VALUES (" + value + ");", Map.of(
                    "str", deleted,
                    "fold", deleted,
                    "neg", negEmpty,
                    "ln", """
                     l    | n   | weight
                    ---------------------
                     -inf | NaN | -1"""));
        }
    }

    @Test
    public void constantsMatchRuntime() {
        this.qst("""
                SELECT CAST(1e0 / -0e0 AS VARCHAR), CAST(ATAN2(0e0, -0e0) AS VARCHAR),
                       CAST(POWER(-0e0, -1e0) AS VARCHAR), CAST(-0e0 AS VARCHAR);
                 a   | b   | c   | d
                ----------------------
                 inf | 0.0 | inf | 0.0
                (1 row)""");
    }

    @Test
    public void zerosAreOneValue() {
        // Grouping, joins, and aggregates see one zero, and print it without a sign
        var ccs = this.getCCS("""
                CREATE TABLE t(x DOUBLE, r REAL);
                CREATE TABLE u(y DOUBLE);
                CREATE VIEW g AS SELECT CAST(x AS VARCHAR) AS s, COUNT(*) AS c FROM t GROUP BY x;
                CREATE VIEW d AS SELECT DISTINCT CAST(r AS VARCHAR) AS s FROM t;
                CREATE VIEW j AS SELECT CAST(x AS VARCHAR) AS s FROM t JOIN u ON x = y;
                CREATE VIEW a AS SELECT CAST(SUM(x) AS VARCHAR) AS sm, CAST(MIN(x) AS VARCHAR) AS mn FROM t;""")
                .withStringTrim();
        ccs.stepMultiView("""
                INSERT INTO t VALUES (CAST('-0' AS DOUBLE), CAST('-0' AS REAL)), (0e0, CAST(0 AS REAL));
                INSERT INTO u VALUES (CAST('-0' AS DOUBLE));""", Map.of(
                "g", """
                 s   | c | weight
                ------------------
                 0.0 | 2 | 1""",
                "d", """
                 s   | weight
                --------------
                 0.0 | 1""",
                "j", """
                 s   | weight
                --------------
                 0.0 | 2""",
                "a", """
                 sm  | mn  | weight
                --------------------
                 0.0 | 0.0 | 1"""));
    }
}
