package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Incremental tests for the covariance family: deleting rows must walk the
 * results back through the same values that inserting them produced. */
public class CovarIncrementalTests extends SqlIoTest {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        options.languageOptions.incrementalize = true;
        return options;
    }

    /** Shrinking a group below two pairs makes COVAR_SAMP NULL; shrinking it
     * below one pair makes every result but REGR_COUNT NULL. */
    @Test
    public void globalAggregate() {
        var ccs = this.getCCS("""
                CREATE TABLE R(id INT, y INT, x INT);
                CREATE VIEW V AS SELECT
                   COVAR_POP(y, x) AS cp, COVAR_SAMP(y, x) AS cs,
                   REGR_SXX(y, x) AS sxx, REGR_SYY(y, x) AS syy,
                   REGR_COUNT(y, x) AS rc
                FROM R;""");
        // Two complete pairs, plus one row that no aggregate may see.
        ccs.step("""
                INSERT INTO R VALUES (1, 1, 10), (2, 3, 30), (3, NULL, 50);""", """
                 cp | cs | sxx | syy | rc | weight
                -----------------------------------
                 10 | 20 | 200 | 2   | 2  | 1""");
        // One complete pair left: COVAR_SAMP has no degrees of freedom.
        ccs.step("""
                REMOVE FROM R VALUES (2, 3, 30);""", """
                 cp | cs   | sxx | syy | rc | weight
                -------------------------------------
                 10 | 20   | 200 | 2   | 2  | -1
                 0  |NULL  | 0   | 0   | 1  | 1""");
        // No complete pair left, but the table is not empty.
        ccs.step("""
                REMOVE FROM R VALUES (1, 1, 10);""", """
                 cp   | cs   | sxx  | syy  | rc | weight
                -----------------------------------------
                 0    |NULL  | 0    | 0    | 1  | -1
                 NULL |NULL  |NULL  |NULL  | 0  | 1""");
        // The table becomes empty, which yields the same result as before.
        ccs.step("""
                REMOVE FROM R VALUES (3, NULL, 50);""", """
                 cp | cs | sxx | syy | rc | weight
                -----------------------------------""");
    }

    /** The same, grouped, so that groups appear and disappear. */
    @Test
    public void groupedAggregate() {
        var ccs = this.getCCS("""
                CREATE TABLE G(k INT, y INT, x INT);
                CREATE VIEW V AS SELECT k, COVAR_POP(y, x) AS cp, REGR_COUNT(y, x) AS rc
                FROM G GROUP BY k;""");
        ccs.step("""
                INSERT INTO G VALUES (1, 1, 10), (1, 3, 30), (2, 4, 20);""", """
                 k | cp | rc | weight
                ----------------------
                 1 | 10 | 2  | 1
                 2 | 0  | 1  | 1""");
        // Group 2 disappears; group 1 loses one of its two pairs.
        ccs.step("""
                REMOVE FROM G VALUES (2, 4, 20);
                REMOVE FROM G VALUES (1, 3, 30);""", """
                 k | cp | rc | weight
                ----------------------
                 1 | 10 | 2  | -1
                 1 | 0  | 1  | 1
                 2 | 0  | 1  | -1""");
    }
}
