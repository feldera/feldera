package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.junit.Test;

/** GC of the inputs of a LEFT JOIN. */
public class LeftJoinGCIncrementalTests extends StreamingTestBase {
    /** Delete the only R row with key 5 once the waterline of L is past 5. */
    void checkRightDelete(String sql) {
        String[] programs = {
                // As written
                sql,
                // Without LATENESS
                sql.replace(" LATENESS 0", "").replace(" LATENESS 100", "")
        };
        for (String program : programs) {
            var ccs = this.getCCS(program).compactAfterEachStep();
            // L row 5 joins R row 5
            ccs.step("INSERT INTO L VALUES(5, 1); INSERT INTO R VALUES(5, 10);", """
                     k | v | x  | weight
                    ---------------------
                     5 | 1 | 10 | 1""");
            // Move the waterline of L past 5; the waterline of R, if any, stays below 5.
            // Three steps, so that compaction merges the batches of R and applies its GC.
            ccs.step("INSERT INTO L VALUES(100, 2); INSERT INTO R VALUES(60, 0);", """
                     k   | v | x  | weight
                    -----------------------
                     100 | 2 |NULL| 1""");
            ccs.step("INSERT INTO L VALUES(101, 3); INSERT INTO R VALUES(61, 0);", """
                     k   | v | x  | weight
                    -----------------------
                     101 | 3 |NULL| 1""");
            ccs.step("INSERT INTO L VALUES(102, 4); INSERT INTO R VALUES(62, 0);", """
                     k   | v | x  | weight
                    -----------------------
                     102 | 4 |NULL| 1""");
            // Delete a record which is not late.  R has no other row with key 5, so (5, 1, NULL) comes back.
            // GC must keep key 5.
            ccs.step("REMOVE FROM R VALUES(5, 10);", """
                     k | v | x  | weight
                    ---------------------
                     5 | 1 | 10 | -1
                     5 | 1 |NULL| 1""");
        }
    }

    /** Only the key of L has LATENESS; the right input has no waterline and is not GC-ed.
     * Expected outputs validated with Postgres. */
    @Test
    public void rightWithoutWaterline() {
        this.checkRightDelete("""
                CREATE TABLE L (k BIGINT NOT NULL LATENESS 0, v INT);
                CREATE TABLE R (k BIGINT NOT NULL, x INT);
                CREATE VIEW V AS SELECT L.k, L.v, R.x FROM L LEFT JOIN R ON L.k = R.k;""");
    }

    /** Both keys have LATENESS, and R lags: the right input is GC-ed below the smaller waterline.
     * Expected outputs validated with Postgres. */
    @Test
    public void rightLagging() {
        this.checkRightDelete("""
                CREATE TABLE L (k BIGINT NOT NULL LATENESS 0, v INT);
                CREATE TABLE R (k BIGINT NOT NULL LATENESS 100, x INT);
                CREATE VIEW V AS SELECT L.k, L.v, R.x FROM L LEFT JOIN R ON L.k = R.k;""");
    }
}
