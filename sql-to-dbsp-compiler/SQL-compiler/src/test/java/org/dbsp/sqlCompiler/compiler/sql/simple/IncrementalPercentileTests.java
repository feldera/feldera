package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Incremental (multi-step) tests for PERCENTILE_CONT and PERCENTILE_DISC.
 *  These tests use incrementalize=true so that each step produces deltas.
 *  Tests cover: NULL handling across steps, all-NULL inputs, extreme value insertion/removal,
 *  and a comprehensive 20-step deep test with group creation/destruction, deletes, and empty table. */
public class IncrementalPercentileTests extends SqlIoTest {
    @Override
    public CompilerOptions testOptions() {
        CompilerOptions options = super.testOptions();
        options.languageOptions.incrementalize = true;
        options.languageOptions.ignoreOrderBy = true;
        return options;
    }

    @Test
    public void testPercentileWithNullsIncremental() {
        var ccs = this.getCCS("""
                CREATE TABLE percentile_nulls (
                    val DOUBLE
                );

                CREATE VIEW v_nulls AS SELECT
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS median_cont,
                    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY val) AS median_disc
                FROM percentile_nulls;
                """);

        // Non-null values: 10, 20, 30, 40 -> CONT=25, DISC=20
        ccs.step("""
                INSERT INTO percentile_nulls VALUES
                    (NULL),
                    (10.0),
                    (NULL),
                    (20.0),
                    (30.0),
                    (NULL),
                    (40.0);
                """, """
                 median_cont | median_disc | weight
                ------------------------------------
                 25          | 20          | 1""");

        // More NULLs — no change
        ccs.step("""
                INSERT INTO percentile_nulls VALUES
                    (NULL),
                    (NULL);
                """, """
                 median_cont | median_disc | weight
                ------------------------------------""");

        // Add 50 — Non-null values: 10, 20, 30, 40, 50 -> CONT=30, DISC=30
        ccs.step("""
                INSERT INTO percentile_nulls VALUES (50.0);
                """, """
                 median_cont | median_disc | weight
                ------------------------------------
                 25          | 20          | -1
                 30          | 30          | 1""");
    }

    @Test
    public void testPercentileAllNulls() {
        var ccs = this.getCCS("""
                CREATE TABLE percentile_all_nulls (
                    val DOUBLE
                );

                CREATE VIEW v_all_nulls AS SELECT
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS median_cont,
                    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY val) AS median_disc
                FROM percentile_all_nulls;
                """);

        // Insert only NULLs — no non-null values, so no output row produced
        ccs.step("""
                INSERT INTO percentile_all_nulls VALUES
                    (NULL),
                    (NULL),
                    (NULL);
                """, """
                 median_cont | median_disc | weight
                ------------------------------------""");

        // Add a non-null value — first result appears
        ccs.step("""
                INSERT INTO percentile_all_nulls VALUES (100.0);
                """, """
                 median_cont | median_disc | weight
                ------------------------------------
                 100         | 100         | 1""");
    }

    @Test
    public void testPercentileExtremeValues() {
        var ccs = this.getCCS("""
                CREATE TABLE percentile_edge (
                    val DOUBLE
                );

                CREATE VIEW v_extreme AS SELECT
                    PERCENTILE_CONT(0.0) WITHIN GROUP (ORDER BY val) AS p0,
                    PERCENTILE_CONT(1.0) WITHIN GROUP (ORDER BY val) AS p100,
                    PERCENTILE_DISC(0.0) WITHIN GROUP (ORDER BY val) AS d0,
                    PERCENTILE_DISC(1.0) WITHIN GROUP (ORDER BY val) AS d100
                FROM percentile_edge;
                """);

        ccs.step("""
                INSERT INTO percentile_edge VALUES
                    (-10.0),
                    (0.0),
                    (10.0),
                    (20.0),
                    (30.0),
                    (100.0),
                    (200.0);
                """, """
                 p0 | p100 | d0 | d100 | weight
                ---------------------------------
                 -10 | 200 | -10 | 200 | 1""");

        // Duplicate inserts — no change in min/max
        ccs.step("""
                INSERT INTO percentile_edge VALUES
                    (10.0),
                    (10.0);
                """, """
                 p0 | p100 | d0 | d100 | weight
                ---------------------------------""");

        // New maximum
        ccs.step("""
                INSERT INTO percentile_edge VALUES (300.0);
                """, """
                 p0 | p100 | d0 | d100 | weight
                ---------------------------------
                 -10 | 200 | -10 | 200 | -1
                 -10 | 300 | -10 | 300 | 1""");
    }

    /** Comprehensive 20-step incremental test exercising inserts, deletes, group
     *  creation/destruction, empty table, extreme values, and duplicate handling.
     *  Uses both PERCENTILE_CONT(0.5) and PERCENTILE_DISC(0.5) with GROUP BY to
     *  test the join path between separate percentile operators.
     *  Each step's expected output is the delta (weight +1 for new rows, -1 for removed rows). */
    @Test
    public void testPercentileDeepIncremental() {
        var ccs = this.getCCS("""
                CREATE TABLE pct_deep (
                    grp INT,
                    val DOUBLE
                );

                CREATE VIEW v_deep AS SELECT
                    grp,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS cont,
                    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY val) AS disc
                FROM pct_deep
                GROUP BY grp;
                """);

        // Step 1: Insert 5 rows in group 1
        // grp1=[10,20,30,40,50] -> CONT=30, DISC=30
        ccs.step("""
                INSERT INTO pct_deep VALUES
                    (1, 10.0), (1, 20.0), (1, 30.0), (1, 40.0), (1, 50.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------
                 1   | 30   | 30   | 1""");

        // Step 2: Insert 3 rows in group 2
        // grp2=[100,200,300] -> CONT=200, DISC=200
        ccs.step("""
                INSERT INTO pct_deep VALUES
                    (2, 100.0), (2, 200.0), (2, 300.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------
                 2   | 200  | 200  | 1""");

        // Step 3: Remove minimum from group 1
        // grp1=[20,30,40,50] -> CONT=35, DISC=30
        ccs.step("""
                REMOVE FROM pct_deep VALUES (1, 10.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------
                 1   | 30   | 30   | -1
                 1   | 35   | 30   | 1""");

        // Step 4: Remove maximum from group 1
        // grp1=[20,30,40] -> CONT=30, DISC=30
        ccs.step("""
                REMOVE FROM pct_deep VALUES (1, 50.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------
                 1   | 35   | 30   | -1
                 1   | 30   | 30   | 1""");

        // Step 5: Add duplicate of median — no change in output
        // grp1=[20,30,30,40] -> CONT=30, DISC=30
        ccs.step("""
                INSERT INTO pct_deep VALUES (1, 30.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------""");

        // Step 6: Remove all from group 2 — group becomes empty
        // grp2=[] -> old value retracted, NULL emitted
        ccs.step("""
                REMOVE FROM pct_deep VALUES (2, 100.0);
                REMOVE FROM pct_deep VALUES (2, 200.0);
                REMOVE FROM pct_deep VALUES (2, 300.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------
                 2   |      |      | 1
                 2   | 200  | 200  | -1""");

        // Step 7: Insert single row in group 3
        // grp3=[999] -> CONT=999, DISC=999
        ccs.step("""
                INSERT INTO pct_deep VALUES (3, 999.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------
                 3   | 999  | 999  | 1""");

        // Step 8: Insert extreme values in group 1 — median stays 30
        // grp1=[-1000000,20,30,30,40,1000000] -> CONT=30, DISC=30
        ccs.step("""
                INSERT INTO pct_deep VALUES (1, -1000000.0), (1, 1000000.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------""");

        // Step 9: Remove extreme values — median still 30
        // grp1=[20,30,30,40] -> CONT=30, DISC=30
        ccs.step("""
                REMOVE FROM pct_deep VALUES (1, -1000000.0);
                REMOVE FROM pct_deep VALUES (1, 1000000.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------""");

        // Step 10: Remove one duplicate 30 — median still 30
        // grp1=[20,30,40] -> CONT=30, DISC=30
        ccs.step("""
                REMOVE FROM pct_deep VALUES (1, 30.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------""");

        // Step 11: Insert negative values, shifting median left
        // grp1=[-100,-50,20,30,40] -> CONT=20, DISC=20
        ccs.step("""
                INSERT INTO pct_deep VALUES (1, -100.0), (1, -50.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------
                 1   | 30   | 30   | -1
                 1   | 20   | 20   | 1""");

        // Step 12: Remove all from group 1 — group becomes empty
        // grp1=[] -> old value retracted, NULL emitted
        ccs.step("""
                REMOVE FROM pct_deep VALUES (1, -100.0);
                REMOVE FROM pct_deep VALUES (1, -50.0);
                REMOVE FROM pct_deep VALUES (1, 20.0);
                REMOVE FROM pct_deep VALUES (1, 30.0);
                REMOVE FROM pct_deep VALUES (1, 40.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------
                 1   |      |      | 1
                 1   | 20   | 20   | -1""");

        // Step 13: Remove all from group 3 — table now completely empty
        // grp3=[] -> old value retracted, NULL emitted
        ccs.step("""
                REMOVE FROM pct_deep VALUES (3, 999.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------
                 3   |      |      | 1
                 3   | 999  | 999  | -1""");

        // Step 14: Rebuild group 1 from empty — NULL retracted, new value emitted
        // grp1=[1,2,3,4,5] -> CONT=3, DISC=3
        ccs.step("""
                INSERT INTO pct_deep VALUES
                    (1, 1.0), (1, 2.0), (1, 3.0), (1, 4.0), (1, 5.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------
                 1   |      |      | -1
                 1   | 3    | 3    | 1""");

        // Step 15: Rebuild group 2 from empty — NULL retracted, new value emitted
        // grp2=[10,20,30,40] -> CONT=25, DISC=20
        ccs.step("""
                INSERT INTO pct_deep VALUES
                    (2, 10.0), (2, 20.0), (2, 30.0), (2, 40.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------
                 2   |      |      | -1
                 2   | 25   | 20   | 1""");

        // Step 16: Remove middle value from group 1
        // grp1=[1,2,4,5] -> CONT=3 (2+0.5*(4-2)=3, unchanged), DISC=2
        ccs.step("""
                REMOVE FROM pct_deep VALUES (1, 3.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------
                 1   | 3    | 3    | -1
                 1   | 3    | 2    | 1""");

        // Step 17: Grow group 2 substantially
        // grp2=[10,20,30,40,50,60,70,80,90,100] -> CONT=55, DISC=50
        ccs.step("""
                INSERT INTO pct_deep VALUES
                    (2, 50.0), (2, 60.0), (2, 70.0), (2, 80.0), (2, 90.0), (2, 100.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------
                 2   | 25   | 20   | -1
                 2   | 55   | 50   | 1""");

        // Step 18: Remove alternating values from group 2
        // grp2=[10,30,50,70,90] -> CONT=50, DISC=50
        ccs.step("""
                REMOVE FROM pct_deep VALUES (2, 20.0);
                REMOVE FROM pct_deep VALUES (2, 40.0);
                REMOVE FROM pct_deep VALUES (2, 60.0);
                REMOVE FROM pct_deep VALUES (2, 80.0);
                REMOVE FROM pct_deep VALUES (2, 100.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------
                 2   | 55   | 50   | -1
                 2   | 50   | 50   | 1""");

        // Step 19: Multi-group change — restore group 1 median, shrink group 2
        // grp1=[1,2,3,4,5] -> CONT=3, DISC=3
        // grp2=[30,50,70,90] -> CONT=60, DISC=50
        ccs.step("""
                INSERT INTO pct_deep VALUES (1, 3.0);
                REMOVE FROM pct_deep VALUES (2, 10.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------
                 1   | 3    | 2    | -1
                 1   | 3    | 3    | 1
                 2   | 50   | 50   | -1
                 2   | 60   | 50   | 1""");

        // Step 20: Add duplicates of median — no change
        // grp1=[1,2,3,3,3,3,4,5] -> CONT=3, DISC=3
        ccs.step("""
                INSERT INTO pct_deep VALUES (1, 3.0), (1, 3.0), (1, 3.0);
                """, """
                 grp | cont | disc | weight
                ------------------------------""");
    }

    /** Focused test verifying that NULL is properly retracted when a group
     *  is rebuilt from empty. Documents the empty-group NULL semantics. */
    @Test
    public void testPercentileEmptyGroupNullBehavior() {
        var ccs = this.getCCS("""
                CREATE TABLE null_test (
                    grp INT,
                    val DOUBLE
                );

                CREATE VIEW v_null_test AS SELECT
                    grp,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS median
                FROM null_test
                GROUP BY grp;
                """);

        // Step 1: Insert values into group 1
        // grp1=[10,20,30] -> median=20
        ccs.step("""
                INSERT INTO null_test VALUES (1, 10.0), (1, 20.0), (1, 30.0);
                """, """
                 grp | median | weight
                -----------------------
                 1   | 20     | 1""");

        // Step 2: Remove all values — group becomes empty
        // grp1=[] -> old value retracted, NULL emitted
        ccs.step("""
                REMOVE FROM null_test VALUES (1, 10.0);
                REMOVE FROM null_test VALUES (1, 20.0);
                REMOVE FROM null_test VALUES (1, 30.0);
                """, """
                 grp | median | weight
                -----------------------
                 1   |        | 1
                 1   | 20     | -1""");

        // Step 3: Re-insert values — NULL retracted, new value emitted
        // grp1=[5,15,25] -> median=15
        ccs.step("""
                INSERT INTO null_test VALUES (1, 5.0), (1, 15.0), (1, 25.0);
                """, """
                 grp | median | weight
                -----------------------
                 1   |        | -1
                 1   | 15     | 1""");

        // Step 4: Verify the cycle works again — remove all
        // grp1=[] -> old value retracted, NULL emitted
        ccs.step("""
                REMOVE FROM null_test VALUES (1, 5.0);
                REMOVE FROM null_test VALUES (1, 15.0);
                REMOVE FROM null_test VALUES (1, 25.0);
                """, """
                 grp | median | weight
                -----------------------
                 1   |        | 1
                 1   | 15     | -1""");

        // Step 5: Rebuild again — NULL retracted
        // grp1=[100] -> median=100
        ccs.step("""
                INSERT INTO null_test VALUES (1, 100.0);
                """, """
                 grp | median | weight
                -----------------------
                 1   |        | -1
                 1   | 100    | 1""");
    }
}
