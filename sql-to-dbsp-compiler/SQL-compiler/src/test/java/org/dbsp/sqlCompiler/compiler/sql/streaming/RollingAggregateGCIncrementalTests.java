package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.junit.Test;

/** GC of rolling aggregates. */
public class RollingAggregateGCIncrementalTests extends StreamingTestBase {
    /** A row at the waterline needs the inputs up to 50 below it, although the window
     * is only 40 wide.  Expected outputs validated with Postgres. */
    @Test
    public void precedingUpperBound() {
        String sql = """
                CREATE TABLE T (ts BIGINT NOT NULL LATENESS 10, x BIGINT NOT NULL);
                CREATE VIEW V AS SELECT ts, x, SUM(x) OVER (ORDER BY ts
                   RANGE BETWEEN 50 PRECEDING AND 10 PRECEDING) AS s FROM T;""";
        String[] programs = {
                // As written
                sql,
                // Without LATENESS
                sql.replace(" LATENESS 10", "")
        };
        for (String program : programs) {
            var ccs = this.getCCS(program).compactAfterEachStep();
            ccs.step("INSERT INTO T VALUES(50, 1);", """
                     ts | x | s   | weight
                    -----------------------
                     50 | 1 |NULL| 1""");
            ccs.step("INSERT INTO T VALUES(100, 0);", """
                     ts  | x | s | weight
                    ----------------------
                     100 | 0 | 1 | 1""");
            // Move the waterline of ts to 95, one row per step, so that compaction applies GC
            for (int ts = 101; ts < 106; ts++)
                ccs.step("INSERT INTO T VALUES(" + ts + ", 0);", """
                         ts | x | s   | weight
                        -----------------------
                         TS | 0 |NULL| 1""".replace("TS", Integer.toString(ts)));
            // Not late; the window of 95 is [45, 85], which contains 50
            ccs.step("INSERT INTO T VALUES(95, 0);", """
                     ts  | x | s   | weight
                    ------------------------
                     95  | 0 | 1   | 1
                     105 | 0 |NULL| -1
                     105 | 0 | 0   | 1""");
        }
    }
}
