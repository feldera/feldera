package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Tests for the MODE aggregate, rewritten by ModeToArgMaxRule.
 * Expected values follow the SQL convention that aggregates ignore NULLs;
 * the test data has a unique mode wherever a tie would make the result
 * implementation-defined.
 * All query results validated using Postgres mode() WITHIN GROUP (ORDER BY ...). */
public class ModeTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation("""
                CREATE TABLE M(
                   k INT,
                   v INT,
                   w INT NOT NULL,
                   c BOOLEAN NOT NULL
                );
                INSERT INTO M VALUES
                   (1, 10, 5, true),
                   (1, 10, 6, false),
                   (1, 20, 6, true),
                   (1, 20, 6, false),
                   (1, 20, 5, true),
                   (2, NULL, 7, true),
                   (2, 30, 7, false),
                   (2, NULL, 8, true),
                   (4, 60, 55, false),
                   (NULL, 40, 9, true),
                   (NULL, 40, 9, true),
                   (NULL, 50, 8, false);
                """);
    }

    @Test
    public void testMode() {
        this.qst("""
                SELECT k, MODE(v) AS m1, MODE(v) AS m2 FROM M GROUP BY k;
                 k    | m1 | m2
                ---------------
                 1    | 20 | 20
                 2    | 30 | 30
                 4    | 60 | 60
                 NULL | 40 | 40
                (4 rows)

                SELECT MODE(v) AS mv, MODE(w) AS mw FROM M;
                 mv | mw
                ---------
                 20 | 6
                (1 row)""");
    }

    @Test
    public void testModeWithOtherAggregates() {
        this.qst("""
                SELECT k, MODE(v) AS mv, MODE(w) AS mw, COUNT(*) AS cnt, SUM(v) AS s
                FROM M GROUP BY k;
                 k    | mv | mw | cnt | s
                ---------------------------
                 1    | 20 | 6  | 5   | 80
                 2    | 30 | 7  | 3   | 30
                 4    | 60 | 55 | 1   | 60
                 NULL | 40 | 9  | 3   | 130
                (4 rows)""");
    }

    @Test
    public void testModeInterleaved() {
        // MODE calls interspersed with other aggregates: exercises the final
        // projection that reassembles columns from the joined branches.
        // The two MODE(v) calls share one branch.
        this.qst("""
                SELECT k, MIN(v) AS mn, MODE(v) AS mv, SUM(w) AS sw, MODE(w) AS mw,
                       COUNT(v) AS cv, MODE(v) AS mv2, MAX(w) AS mx
                FROM M GROUP BY k;
                 k    | mn | mv | sw | mw | cv | mv2 | mx
                -------------------------------------------
                 1    | 10 | 20 | 28 | 6  | 5  | 20  | 6
                 2    | 30 | 30 | 22 | 7  | 1  | 30  | 8
                 4    | 60 | 60 | 55 | 55 | 1  | 60  | 55
                 NULL | 40 | 40 | 26 | 9  | 3  | 40  | 9
                (4 rows)""");
    }

    @Test
    public void testModeFilter() {
        // Group 2 has only NULL values passing the filter; group 4 has no
        // passing rows at all.  Both must produce NULL and keep their group.
        this.qst("""
                SELECT k, MODE(v) FILTER (WHERE c) AS mf, COUNT(*) AS cnt
                FROM M GROUP BY k;
                 k    | mf   | cnt
                ------------------
                 1    | 20   | 5
                 2    | NULL | 3
                 4    | NULL | 1
                 NULL | 40   | 3
                (4 rows)""");
    }

    @Test
    public void testModeEmptyInput() {
        this.qst("""
                SELECT MODE(v) AS m FROM M WHERE FALSE;
                 m
                ------
                 NULL
                (1 row)""");
    }

    @Test
    public void testModeDistinctRejected() {
        this.queryFailingInCompilation("SELECT MODE(DISTINCT v) FROM M",
                "MODE does not support DISTINCT");
    }
}
