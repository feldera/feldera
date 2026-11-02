package org.dbsp.sqlCompiler.compiler.sql.recursive;

import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.junit.Test;

import java.util.Map;

/** ShareIndexes must not merge MapIndexes that read different outputs of a
 * multi-output operator (here the nested operator of two mutually recursive views). */
public class SharedIndexPortTests extends BaseSQLTests {
    static final String RECURSION = """
            CREATE TABLE src (k BIGINT NOT NULL, v BIGINT NOT NULL);
            DECLARE RECURSIVE VIEW a (k BIGINT NOT NULL, v BIGINT NOT NULL);
            DECLARE RECURSIVE VIEW b (k BIGINT NOT NULL, v BIGINT NOT NULL);
            CREATE VIEW a AS
              SELECT k, v FROM src WHERE k = 1
              UNION SELECT s.k, s.v FROM src s JOIN b ON s.k = b.k + 1;
            CREATE VIEW b AS SELECT k, v * 10 + 1 AS v FROM a;
            """;

    static final String A = """
             k | v  | weight
            -----------------
             1 | 10 | 1
             2 | 20 | 1
             3 | 30 | 1""";
    static final String B = """
             k | v   | weight
            ------------------
             1 | 101 | 1
             2 | 201 | 1
             3 | 301 | 1""";

    @Test
    public void crossJoinOutputs() {
        var ccs = this.getCCS("CREATE TABLE d1 (day_no BIGINT NOT NULL);\n" + RECURSION + """
                CREATE VIEW out_a AS SELECT d.day_no, x.* FROM a x CROSS JOIN d1 d;
                CREATE VIEW out_b AS SELECT d.day_no, x.* FROM b x CROSS JOIN d1 d;""");
        ccs.stepMultiView("""
                INSERT INTO src VALUES (1, 10), (2, 20), (3, 30);
                INSERT INTO d1 VALUES (7);""", Map.of(
                "a", A, "b", B,
                "out_a", """
                 day_no | k | v  | weight
                --------------------------
                 7      | 1 | 10 | 1
                 7      | 2 | 20 | 1
                 7      | 3 | 30 | 1""",
                "out_b", """
                 day_no | k | v   | weight
                ---------------------------
                 7      | 1 | 101 | 1
                 7      | 2 | 201 | 1
                 7      | 3 | 301 | 1"""));
    }

    @Test
    public void equiJoinOutputs() {
        var ccs = this.getCCS("CREATE TABLE t (k BIGINT NOT NULL, w BIGINT NOT NULL);\n" + RECURSION + """
                CREATE VIEW out_a AS SELECT x.k, x.v, t.w FROM a x JOIN t ON x.k = t.k;
                CREATE VIEW out_b AS SELECT x.k, x.v, t.w FROM b x JOIN t ON x.k = t.k;""");
        ccs.stepMultiView("""
                INSERT INTO src VALUES (1, 10), (2, 20), (3, 30);
                INSERT INTO t VALUES (2, 5);""", Map.of(
                "a", A, "b", B,
                "out_a", """
                 k | v  | w | weight
                ---------------------
                 2 | 20 | 5 | 1""",
                "out_b", """
                 k | v   | w | weight
                ----------------------
                 2 | 201 | 5 | 1"""));
    }
}
