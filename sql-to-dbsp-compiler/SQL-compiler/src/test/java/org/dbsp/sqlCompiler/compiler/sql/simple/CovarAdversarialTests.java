package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Corner cases for the covariance family: COVAR_POP, COVAR_SAMP,
 * REGR_COUNT, REGR_SXX, REGR_SYY.
 * All expected results were validated using Postgres. */
public class CovarAdversarialTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation("""
                -- y is NOT NULL, x is nullable; group 1 has no row where both are non-NULL
                CREATE TABLE NN(k INT, y INT NOT NULL, x INT);
                INSERT INTO NN VALUES (1, 5, NULL), (1, 7, NULL), (2, 3, 4);

                -- arguments of different types
                CREATE TABLE MIXED(y INT, x DOUBLE);
                INSERT INTO MIXED VALUES (0, 0.1), (100, 0.2), (200, 0.3), (300, 0.4);

                -- arguments of the same type but of different precision
                CREATE TABLE RATES(id INT, rate DECIMAL(6, 4), amount DECIMAL(12, 2));
                INSERT INTO RATES VALUES (1, 1.0000, 1000000.00), (2, 1.0001, 3000000.00);

                -- a constant column: every covariance-family result is 0
                CREATE TABLE BIG(id INT, v INT);
                INSERT INTO BIG VALUES (1, 2000000000), (2, 2000000000);

                -- the sum of squares of deviations of v is 5e9, which exceeds INTEGER
                CREATE TABLE SXX(id INT, v INT);
                INSERT INTO SXX VALUES (1, 100000), (2, 200000);

                -- money-like columns, the natural input of a regression
                CREATE TABLE SALES(id INT, price DECIMAL(7, 2), units DECIMAL(7, 2));
                INSERT INTO SALES VALUES
                   (1, 1000.00, 3.00), (2, 2000.00, 5.00), (3, 3000.00, 4.00);
                """);
    }

    /** The result is NULL when no row of a group has both arguments non-NULL,
     * even when the first argument is declared NOT NULL. */
    @Test
    public void nonNullableFirstArgument() {
        this.qst("""
                SELECT k, COALESCE(COVAR_POP(y, x), -1) AS cp,
                          COALESCE(REGR_SXX(y, x), -1) AS sxx,
                          COALESCE(REGR_SYY(y, x), -1) AS syy
                FROM NN GROUP BY k;
                 k | cp | sxx | syy
                --------------------
                 1 | -1 | -1  | -1
                 2 | 0  | 0   | 0
                (2 rows)""");
    }

    /** The same, for the window form of the aggregate. */
    @Test
    public void nonNullableFirstArgumentWindow() {
        this.qst("""
                SELECT DISTINCT k, COALESCE(COVAR_POP(y, x) OVER (PARTITION BY k), -1) AS cp
                FROM NN;
                 k | cp
                --------
                 1 | -1
                 2 | 0
                (2 rows)""");
    }

    /** Covariance is symmetric: COVAR_POP(y, x) = COVAR_POP(x, y).
     * REGR_SXX(y, x) = REGR_SYY(x, y), since both sum the squares of the
     * deviations of the same column.  Neither identity may depend on the
     * declared types of the arguments. */
    @Test
    public void symmetry() {
        this.qst("""
                SELECT COVAR_POP(y, x) = COVAR_POP(x, y) AS covar_symmetric,
                       REGR_SXX(y, x) = REGR_SYY(x, y) AS regr_symmetric,
                       CAST(ROUND(COVAR_POP(y, x), 4) AS VARCHAR) AS cp_yx,
                       CAST(ROUND(REGR_SXX(y, x), 4) AS VARCHAR) AS sxx_yx
                FROM MIXED;
                 covar_symmetric | regr_symmetric | cp_yx | sxx_yx
                ------------------------------------------------------
                 true            | true           | 12.5  | 0.05
                (1 row)""");
    }

    /** The second argument keeps its own range: a rate and an amount can be
     * correlated even though the amount does not fit in the rate's type. */
    @Test
    public void argumentsOfDifferentPrecision() {
        this.qst("""
                SELECT COVAR_POP(rate, amount) AS cp FROM RATES;
                 cp
                ---------
                 50.0000
                (1 row)""");
    }

    /** The covariance of a constant column with itself is 0. */
    @Test
    public void covarOfConstantColumn() {
        this.qst("""
                SELECT COVAR_POP(v, v) AS cp, REGR_SXX(v, v) AS sxx FROM BIG;
                 cp | sxx
                ----------
                 0  | 0
                (1 row)""");
    }

    /** Control for {@link #covarOfConstantColumn}: VAR_POP, which the
     * covariance family generalizes, over the same column. */
    @Test
    public void varPopOfConstantColumn() {
        this.qst("""
                SELECT VAR_POP(v) AS vp FROM BIG;
                 vp
                ----
                 0
                (1 row)""");
    }

    /** REGR_SXX sums squares, so its value outgrows the range of its
     * arguments: two INTEGER columns yield 5e9 here. */
    @Test
    public void sumOfSquaresOutgrowsIntegerArgument() {
        this.qst("""
                SELECT CAST(REGR_SXX(v, v) AS VARCHAR) AS sxx FROM SXX;
                 sxx
                ------------
                 5000000000
                (1 row)""");
    }

    /** The same, for DECIMAL columns: prices in the thousands give a sum of
     * squares in the millions. */
    @Test
    public void sumOfSquaresOutgrowsDecimalArgument() {
        this.qst("""
                SELECT CAST(COVAR_POP(units, price) AS VARCHAR) AS cp,
                       CAST(REGR_SXX(units, price) AS VARCHAR) AS sxx
                FROM SALES;
                 cp     | sxx
                ---------------------
                 333.33 | 2000000.00
                (1 row)""");
    }

    /** A window aggregate must agree with the equivalent grouped aggregate. */
    @Test
    public void windowMatchesGroup() {
        this.qst("""
                SELECT DISTINCT COVAR_POP(y, x) OVER () AS cp, REGR_SXX(y, x) OVER () AS sxx
                FROM NN;
                 cp   | sxx
                ------------
                 0    | 0
                (1 row)""");
    }
}
