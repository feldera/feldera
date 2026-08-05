package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Tests for the covariance family: COVAR_POP, COVAR_SAMP,
 * REGR_COUNT, REGR_SXX, REGR_SYY.
 * The test data produces exact integer results, so INTEGER and DOUBLE
 * queries compare without rounding.
 * All query results validated using Postgres. */
public class CovarTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        // Group 1 also has rows where only one argument is NULL;
        // group 2 has a single complete row; group 3 has none.
        compiler.submitStatementsForCompilation("""
                CREATE TABLE C(
                   k INT,
                   y INT,
                   x INT,
                   c BOOLEAN NOT NULL
                );
                INSERT INTO C VALUES
                   (1, 1, 10, true),
                   (1, 3, 30, true),
                   (1, NULL, 50, true),
                   (1, 5, NULL, false),
                   (2, 4, 20, true),
                   (2, NULL, NULL, true),
                   (3, NULL, 7, true),
                   (NULL, 1, 18, true),
                   (NULL, 11, 22, false);
                """);
    }

    @Test
    public void testCovarGrouped() {
        // Rows where either argument is NULL are ignored; a group with a
        // single complete row has COVAR_SAMP NULL; a group with no complete
        // rows has all results NULL except REGR_COUNT, which is 0.
        this.qst("""
                SELECT k, COVAR_POP(y, x) AS cp, COVAR_SAMP(y, x) AS cs,
                       REGR_SXX(y, x) AS sxx, REGR_SYY(y, x) AS syy,
                       REGR_COUNT(y, x) AS rc
                FROM C GROUP BY k;
                 k    | cp   | cs   | sxx  | syy  | rc
                --------------------------------------
                 1    | 10   | 20   | 200  | 2    | 2
                 2    | 0    | NULL | 0    | 0    | 1
                 3    | NULL | NULL | NULL | NULL | 0
                 NULL | 10   | 20   | 8    | 50   | 2
                (4 rows)""");
    }

    @Test
    public void testCovarGlobal() {
        this.qst("""
                SELECT COVAR_POP(y, x) AS cp, COVAR_SAMP(y, x) AS cs,
                       REGR_SXX(y, x) AS sxx, REGR_SYY(y, x) AS syy,
                       REGR_COUNT(y, x) AS rc
                FROM C;
                 cp | cs | sxx | syy | rc
                --------------------------
                 8  | 10 | 208 | 68  | 5
                (1 row)

                SELECT COVAR_POP(y, x) AS cp, REGR_COUNT(y, x) AS rc
                FROM C WHERE FALSE;
                 cp   | rc
                -----------
                 NULL | 0
                (1 row)""");
    }

    @Test
    public void testCovarFilter() {
        this.qst("""
                SELECT k, COVAR_POP(y, x) FILTER (WHERE c) AS cpf,
                       COVAR_SAMP(y, x) FILTER (WHERE c) AS csf,
                       REGR_COUNT(y, x) FILTER (WHERE c) AS rcf,
                       COUNT(*) AS cnt
                FROM C GROUP BY k;
                 k    | cpf  | csf  | rcf | cnt
                --------------------------------
                 1    | 10   | 20   | 2   | 4
                 2    | 0    | NULL | 1   | 2
                 3    | NULL | NULL | 0   | 1
                 NULL | 0    | NULL | 1   | 2
                (4 rows)""");
    }

    @Test
    public void testCovarDouble() {
        // DOUBLE arguments use the non-linear implementation.
        // The data yields exact integral values; results are cast to VARCHAR
        // to avoid rounding differences.
        this.qst("""
                SELECT k, CAST(COVAR_POP(CAST(y AS DOUBLE), CAST(x AS DOUBLE)) AS VARCHAR) AS cp,
                       CAST(COVAR_SAMP(CAST(y AS DOUBLE), CAST(x AS DOUBLE)) AS VARCHAR) AS cs,
                       CAST(REGR_SXX(CAST(y AS DOUBLE), CAST(x AS DOUBLE)) AS VARCHAR) AS sxx,
                       CAST(REGR_SYY(CAST(y AS DOUBLE), CAST(x AS DOUBLE)) AS VARCHAR) AS syy
                FROM C GROUP BY k;
                 k    | cp   | cs   | sxx   | syy
                -----------------------------------
                 1    | 10.0 | 20.0 | 200.0 | 2.0
                 2    | 0.0  |NULL  | 0.0   | 0.0
                 3    |NULL  |NULL  |NULL   |NULL
                 NULL | 10.0 | 20.0 | 8.0   | 50.0
                (4 rows)""");
    }

    @Test
    public void nullableTest() {
        var ccs = this.getCCS("""
                CREATE TABLE T(k INT, y INT NOT NULL, x INT);
                CREATE VIEW V AS SELECT k, COVAR_POP(y, x) FROM T GROUP BY k;""");
        ccs.stepWeightOne("INSERT INTO T VALUES(1, 5, NULL);", """
                 k | covar 
                ------------
                 1 | NULL""");
    }

    @Test
    public void coercionTest() {
        var ccs = this.getCCS("""
                CREATE TABLE T(y INT, x DOUBLE);
                CREATE VIEW V AS SELECT COVAR_POP(y, x), COVAR_POP(x, y) FROM T;""");
        ccs.stepWeightOne("INSERT INTO T VALUES (0, 0.1), (100, 0.2), (200, 0.3), (300, 0.4);", """
                 c1   | c2 
                -------------
                 12.5 | 12.5""");
    }
}
