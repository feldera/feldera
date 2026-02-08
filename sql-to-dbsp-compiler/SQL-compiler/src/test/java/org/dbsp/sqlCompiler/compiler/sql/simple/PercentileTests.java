package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Tests for PERCENTILE_CONT and PERCENTILE_DISC aggregate functions (non-incremental).
 *  Tests cover: basic usage, type variations, GROUP BY, edge cases, NULL handling,
 *  multi-percentile queries, and subquery usage.
 *  See IncrementalPercentileTests for incremental (multi-step) tests. */
public class PercentileTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation("""
                CREATE TABLE T(
                   B BIGINT,
                   I INTEGER,
                   S SMALLINT,
                   T TINYINT,
                   R REAL,
                   D DOUBLE,
                   E DECIMAL(3,2),
                   V VARCHAR
                );
                INSERT INTO T VALUES
                   (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                   (0, 0, 0, 0, 0, 0, 0, '0'),
                   (1, 1, 1, 1, 1, 1, 1, '1'),
                   (2, 2, 2, 2, 2, 2, 2, '2');
                """);
    }

    // ---- Basic PERCENTILE_CONT ----

    @Test
    public void testPercentileCont() {
        this.qs("""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY D) AS result
                FROM T;
                 result
                --------
                 1
                (1 row)

                SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY D) AS result
                FROM T;
                 result
                --------
                 0.5
                (1 row)

                SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY D) AS result
                FROM T;
                 result
                --------
                 1.5
                (1 row)

                SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY R) AS result
                FROM T;
                 result
                --------
                 1.8
                (1 row)""");
    }

    // ---- Basic PERCENTILE_DISC ----

    @Test
    public void testPercentileDisc() {
        this.qs("""
                SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY I) AS result
                FROM T;
                 result
                --------
                 1
                (1 row)

                SELECT PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY I) AS result
                FROM T;
                 result
                --------
                 0
                (1 row)

                SELECT PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY I) AS result
                FROM T;
                 result
                --------
                 2
                (1 row)

                SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY B) AS result
                FROM T;
                 result
                --------
                 2
                (1 row)""");
    }

    // ---- DESC ordering ----

    @Test
    public void testPercentileDescending() {
        this.qs("""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY D DESC) AS result
                FROM T;
                 result
                --------
                 1
                (1 row)

                SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY I DESC) AS result
                FROM T;
                 result
                --------
                 1
                (1 row)""");
    }

    // ---- GROUP BY ----

    @Test
    public void testPercentileGroupBy() {
        var ccs = this.getCCS("""
                CREATE TABLE percentile_grp (
                    grp INT,
                    val DOUBLE
                );

                CREATE VIEW v_grouped AS SELECT
                    grp,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS median_cont,
                    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY val) AS median_disc
                FROM percentile_grp
                GROUP BY grp;
                """);

        ccs.step("""
                INSERT INTO percentile_grp VALUES
                    (1, -10.0),
                    (1, 0.0),
                    (1, 10.0),
                    (1, 20.0),
                    (1, 30.0),
                    (2, 100.0),
                    (2, 200.0);
                """, """
                 grp | median_cont | median_disc | weight
                ------------------------------------------
                 1   | 10          | 10          | 1
                 2   | 150         | 100         | 1""");
    }

    @Test
    public void testPercentileGroupByWithEmpty() {
        var ccs = this.getCCS("""
                CREATE TABLE grp_test (
                    category VARCHAR,
                    val DOUBLE
                );

                CREATE VIEW v_grp_test AS SELECT
                    category,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS median
                FROM grp_test
                GROUP BY category;
                """);

        ccs.step("""
                INSERT INTO grp_test VALUES
                    ('A', 10.0),
                    ('A', 20.0),
                    ('A', 30.0),
                    ('B', 100.0),
                    ('C', 1.0),
                    ('C', 2.0),
                    ('C', 3.0),
                    ('C', 4.0),
                    ('C', 5.0);
                """, """
                 category | median | weight
                ----------------------------
                 A| 20     | 1
                 B| 100    | 1
                 C| 3      | 1""");
    }

    // ---- Edge cases ----

    @Test
    public void testPercentileSingleValue() {
        var ccs = this.getCCS("""
                CREATE TABLE single_val (val DOUBLE);

                CREATE VIEW v_single AS SELECT
                    PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY val) AS p0,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS p50,
                    PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY val) AS p100,
                    PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY val) AS d0,
                    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY val) AS d50,
                    PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY val) AS d100
                FROM single_val;
                """);

        ccs.step("""
                INSERT INTO single_val VALUES (42.0);
                """, """
                 p0 | p50 | p100 | d0 | d50 | d100 | weight
                --------------------------------------------
                 42 | 42  | 42   | 42 | 42  | 42   | 1""");
    }

    @Test
    public void testPercentileTwoValues() {
        var ccs = this.getCCS("""
                CREATE TABLE two_vals (val DOUBLE);

                CREATE VIEW v_two AS SELECT
                    PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY val) AS p0,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY val) AS p25,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS p50,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY val) AS p75,
                    PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY val) AS p100
                FROM two_vals;
                """);

        ccs.step("""
                INSERT INTO two_vals VALUES (0.0), (100.0);
                """, """
                 p0 | p25 | p50 | p75 | p100 | weight
                ---------------------------------------
                 0  | 25  | 50  | 75  | 100  | 1""");
    }

    @Test
    public void testPercentileAllSameValues() {
        var ccs = this.getCCS("""
                CREATE TABLE same_vals (val DOUBLE);

                CREATE VIEW v_same AS SELECT
                    PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY val) AS p0,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS p50,
                    PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY val) AS p100,
                    PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY val) AS d0,
                    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY val) AS d50,
                    PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY val) AS d100
                FROM same_vals;
                """);

        ccs.step("""
                INSERT INTO same_vals VALUES (7.0), (7.0), (7.0), (7.0), (7.0);
                """, """
                 p0 | p50 | p100 | d0 | d50 | d100 | weight
                --------------------------------------------
                 7  | 7   | 7    | 7  | 7   | 7    | 1""");
    }

    @Test
    public void testPercentileNearExtremes() {
        var ccs = this.getCCS("""
                CREATE TABLE pct_extreme (val DOUBLE);

                CREATE VIEW v_near_extreme AS SELECT
                    PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY val) AS p1,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY val) AS p99
                FROM pct_extreme;
                """);

        ccs.step("""
                INSERT INTO pct_extreme VALUES (0.0), (25.0), (50.0), (75.0), (100.0);
                """, """
                 p1 | p99 | weight
                -------------------
                 1  | 99  | 1""");
    }

    // ---- NULL handling ----

    @Test
    public void testPercentileWithNulls() {
        // Table T has one NULL row and 3 non-null rows (0, 1, 2)
        // The median of [0, 1, 2] for CONT is 1.0, for DISC is 1
        this.qs("""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY D) AS result
                FROM T;
                 result
                --------
                 1
                (1 row)

                SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY I) AS result
                FROM T;
                 result
                --------
                 1
                (1 row)""");
    }

    // ---- Type variations ----

    @Test
    public void testPercentileIntegerTypes() {
        // PERCENTILE_DISC works directly on integer types.
        this.qs("""
                SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY B) AS bigint_result
                FROM T;
                 bigint_result
                ---------------
                 1
                (1 row)

                SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY S) AS smallint_result
                FROM T;
                 smallint_result
                -----------------
                 1
                (1 row)

                SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY I) AS int_result
                FROM T;
                 int_result
                ------------
                 1
                (1 row)

                SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY T) AS tinyint_result
                FROM T;
                 tinyint_result
                ----------------
                 1
                (1 row)""");
    }

    @Test
    public void testPercentileContOnInteger() {
        // PERCENTILE_CONT auto-casts integer ORDER BY values to DOUBLE and returns DOUBLE.
        // T has non-null values: 0, 1, 2 for each integer column.
        this.qs("""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY I) AS int_result
                FROM T;
                 int_result
                ------------
                 1
                (1 row)

                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY B) AS bigint_result
                FROM T;
                 bigint_result
                ---------------
                 1
                (1 row)

                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY S) AS smallint_result
                FROM T;
                 smallint_result
                -----------------
                 1
                (1 row)

                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY T) AS tinyint_result
                FROM T;
                 tinyint_result
                ----------------
                 1
                (1 row)""");
    }

    @Test
    public void testPercentileContOnIntegerInterpolation() {
        // Verify PERCENTILE_CONT actually interpolates between integer values
        // (cast to DOUBLE), not just returning discrete values.
        var ccs = this.getCCS("""
                CREATE TABLE int_data (grp INT, val INT);

                CREATE VIEW v_int AS SELECT
                    grp,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS median
                FROM int_data
                GROUP BY grp;
                """);

        // [10, 20, 30] -> median = 20.0
        ccs.step("""
                INSERT INTO int_data VALUES (1, 10), (1, 20), (1, 30);
                """, """
                 grp | median | weight
                -----------------------
                 1   | 20     | 1""");
    }

    @Test
    public void testPercentileDecimalType() {
        // PERCENTILE_DISC works directly on DECIMAL.
        this.qs("""
                SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY E) AS decimal_result
                FROM T;
                 decimal_result
                ----------------
                 1.00
                (1 row)

                SELECT PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY E) AS q1
                FROM T;
                 q1
                ------
                 0.00
                (1 row)

                SELECT PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY E) AS q3
                FROM T;
                 q3
                ------
                 2.00
                (1 row)""");
    }

    @Test
    public void testPercentileContOnDecimal() {
        // PERCENTILE_CONT auto-casts DECIMAL ORDER BY values to DOUBLE and returns DOUBLE.
        // T has non-null DECIMAL values: 0.00, 1.00, 2.00
        this.qs("""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY E) AS decimal_result
                FROM T;
                 decimal_result
                ----------------
                 1
                (1 row)

                SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY E) AS q1
                FROM T;
                 q1
                ------
                 0.5
                (1 row)

                SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY E) AS q3
                FROM T;
                 q3
                ------
                 1.5
                (1 row)""");
    }

    @Test
    public void testPercentileRealType() {
        this.qs("""
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY R) AS real_result
                FROM T;
                 real_result
                -------------
                 1
                (1 row)

                SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY R) AS real_result
                FROM T;
                 real_result
                -------------
                 1
                (1 row)""");
    }

    // ---- Multiple percentiles in same GROUP BY ----

    @Test
    public void testMultiplePercentileSameGroupBy() {
        var ccs = this.getCCS("""
                CREATE TABLE multi_pct (val DOUBLE);

                CREATE VIEW v_multi AS SELECT
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY val) AS p25,
                    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY val) AS p50,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY val) AS p75,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY val) AS p99
                FROM multi_pct;
                """);

        ccs.step("""
                INSERT INTO multi_pct VALUES (0.0), (10.0), (20.0), (30.0), (40.0);
                """, """
                 p25  | p50  | p75  | p99  | weight
                ---------------------------------------
                 10   | 20   | 30   | 39.6 | 1""");
    }

    @Test
    public void testMultiplePercentileMixedContDisc() {
        var ccs = this.getCCS("""
                CREATE TABLE mixed_pct (val DOUBLE);

                CREATE VIEW v_mixed AS SELECT
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS cont_50,
                    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY val) AS disc_50
                FROM mixed_pct;
                """);

        ccs.step("""
                INSERT INTO mixed_pct VALUES (10.0), (20.0), (30.0), (40.0);
                """, """
                 cont_50 | disc_50 | weight
                ----------------------------
                 25      | 20      | 1""");
    }

    @Test
    public void testMultiplePercentileGroupedBy() {
        var ccs = this.getCCS("""
                CREATE TABLE grp_multi_pct (
                    grp INT,
                    val DOUBLE
                );

                CREATE VIEW v_grp_multi AS SELECT
                    grp,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY val) AS p25,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY val) AS p75
                FROM grp_multi_pct
                GROUP BY grp;
                """);

        ccs.step("""
                INSERT INTO grp_multi_pct VALUES
                    (1, 0.0), (1, 10.0), (1, 20.0), (1, 30.0),
                    (2, 100.0), (2, 200.0);
                """, """
                 grp | p25  | p75  | weight
                -----------------------------
                 1   | 7.5  | 22.5 | 1
                 2   | 125  | 175  | 1""");
    }

    // ---- Subquery ----

    @Test
    public void testPercentileOnAggregatedSubquery() {
        this.compileRustTestCase("""
                CREATE TABLE contracts (
                    project_id bigint
                );

                CREATE MATERIALIZED VIEW meta_contracts_per_project (
                    p50
                ) AS
                SELECT
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cnt) as p50
                FROM (
                    SELECT project_id, COUNT(*)::double as cnt
                    FROM contracts
                    GROUP BY project_id
                ) t;
                """);
    }

    // ---- Mixed percentile + regular aggregates ----

    @Test
    public void testMixedPercentileAndRegularAggregates() {
        var ccs = this.getCCS("""
                CREATE TABLE mixed_agg (
                    grp INT,
                    val DOUBLE
                );

                CREATE VIEW v_mixed_agg AS SELECT
                    grp,
                    COUNT(*) AS cnt,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS median,
                    AVG(val) AS avg_val
                FROM mixed_agg
                GROUP BY grp;
                """);

        ccs.step("""
                INSERT INTO mixed_agg VALUES
                    (1, 10.0), (1, 20.0), (1, 30.0), (1, 40.0),
                    (2, 100.0), (2, 200.0);
                """, """
                 grp | cnt | median | avg_val | weight
                ----------------------------------------
                 1   | 4   | 25     | 25      | 1
                 2   | 2   | 150    | 150     | 1""");
    }

    @Test
    public void testMixedPercentileFirstOrdering() {
        var ccs = this.getCCS("""
                CREATE TABLE mixed_order (
                    grp INT,
                    val DOUBLE
                );

                CREATE VIEW v_mixed_order AS SELECT
                    grp,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS median,
                    COUNT(*) AS cnt
                FROM mixed_order
                GROUP BY grp;
                """);

        ccs.step("""
                INSERT INTO mixed_order VALUES
                    (1, 10.0), (1, 20.0), (1, 30.0);
                """, """
                 grp | median | cnt | weight
                --------------------------------
                 1   | 20     | 3   | 1""");
    }
}
