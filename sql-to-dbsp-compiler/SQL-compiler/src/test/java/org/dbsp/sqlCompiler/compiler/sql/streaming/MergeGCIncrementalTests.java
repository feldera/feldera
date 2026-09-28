package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainNValuesOperator;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

/** MergeGC shares a trace between consumers only when they retain it with the same bounds. */
public class MergeGCIncrementalTests extends StreamingTestBase {
    /** T joins S1 and S2 on ts; each join retains T with the waterline of the other side. */
    static final String TWO_JOINS = """
            CREATE TABLE T (ts INT NOT NULL, v INT);
            CREATE TABLE S1 (ts INT NOT NULL LATENESS 0, x INT);
            CREATE TABLE S2 (ts INT NOT NULL LATENESS 100, y INT);
            CREATE LOCAL VIEW V1 AS SELECT T.ts, T.v, S1.x FROM T JOIN S1 ON T.ts = S1.ts;
            CREATE LOCAL VIEW V2 AS SELECT T.ts, T.v, S2.y FROM T JOIN S2 ON T.ts = S2.ts;
            CREATE VIEW V AS SELECT 1 AS w, ts, v, x AS o FROM V1
            UNION ALL SELECT 2 AS w, ts, v, y AS o FROM V2;""";

    /** The two retain operators of T have different bounds, so they are not merged. */
    @Test
    public void differentBoundsNotMerged() {
        CompilerCircuit cc = this.getCC(TWO_JOINS);
        cc.visit(new CircuitVisitor(cc.compiler) {
            int retainKeys = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.retainKeys++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(2, this.retainKeys);
            }
        });
    }

    /** A row of T that is below the waterline of S1 must still join with S2.  The program runs
     * with and without LATENESS; no input row is late.  Expected outputs validated with Postgres. */
    @Test
    public void differentBoundsStep() {
        String[] programs = {
                // As written
                TWO_JOINS,
                // Without LATENESS
                TWO_JOINS.replace(" LATENESS 0", "").replace(" LATENESS 100", "")
        };
        for (String program : programs) {
            var ccs = this.getCCS(program).compactAfterEachStep();
            ccs.step("INSERT INTO T VALUES(50, 1);", """
                     w | ts | v | o | weight
                    -------------------------""");
            ccs.step("INSERT INTO S1 VALUES(200, 1); INSERT INTO S2 VALUES(120, 0);", """
                     w | ts | v | o | weight
                    -------------------------""");
            // Batches that do not join, so that compaction merges the trace of T
            for (int i = 0; i < 10; i++)
                ccs.step("INSERT INTO S1 VALUES(" + (201 + i) + ", 2); INSERT INTO T VALUES(" + (1000 + i) + ", 0);", """
                         w | ts | v | o | weight
                        -------------------------""");
            // The waterline of S2 is 20, so 50 is not late
            ccs.step("INSERT INTO S2 VALUES(50, 9);", """
                     w | ts | v | o | weight
                    -------------------------
                     2 | 50 | 1 | 9 | 1""");
        }
    }

    /** Both views compute MAX(x) grouped by ts, so the two retain operators of x keep the same
     * values with the same bounds, and they are merged. */
    @Test
    public void samePolicyMerged() {
        CompilerCircuit cc = this.getCC("""
                CREATE TABLE T (ts INT, x INT LATENESS 2, y INT);
                CREATE VIEW V1 AS SELECT ts, MAX(x) FROM T GROUP BY ts;
                CREATE VIEW V2 AS SELECT ts, MAX(x), SUM(y) FROM T GROUP BY ts;""");
        cc.visit(new CircuitVisitor(cc.compiler) {
            int retainNValues = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
                this.retainNValues++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.retainNValues);
            }
        });
    }

    /** MIN retains the smallest values below the waterline and MAX the largest; their retain
     * operators have the same function and bounds but must not be merged.  The program runs with
     * and without LATENESS; no input row is late.  Expected outputs validated with Postgres. */
    @Test
    public void minMaxNotMerged() {
        String sql = """
                CREATE TABLE T (ts INT, x INT LATENESS 2);
                CREATE VIEW V AS SELECT ts, MIN(x) AS mn, MAX(x) AS mx FROM T GROUP BY ts;""";
        String[] programs = {
                // As written
                sql,
                // Without LATENESS
                sql.replace(" LATENESS 2", "")
        };
        for (String program : programs) {
            var ccs = this.getCCS(program).compactAfterEachStep();
            ccs.step("INSERT INTO T VALUES(1, 5), (1, 6), (1, 7);", """
                     ts | mn | mx | weight
                    -----------------------
                     1  | 5  | 7  | 1""");
            ccs.step("INSERT INTO T VALUES(2, 100);", """
                     ts | mn  | mx  | weight
                    -------------------------
                     2  | 100 | 100 | 1""");
            // The waterline of x is 99; all values of group 1 are below it
            ccs.step("INSERT INTO T VALUES(2, 101);", """
                     ts | mn  | mx  | weight
                    -------------------------
                     2  | 100 | 100 | -1
                     2  | 100 | 101 | 1""");
            // Batches that change no output, so that compaction merges the traces
            for (int i = 0; i < 6; i++)
                ccs.step("INSERT INTO T VALUES(2, 100);", """
                         ts | mn | mx | weight
                        -----------------------""");
            ccs.step("INSERT INTO T VALUES(1, 99);", """
                     ts | mn | mx | weight
                    -----------------------
                     1  | 5  | 7  | -1
                     1  | 5  | 99 | 1""");
            // MAX needs 7, the largest value below the waterline
            ccs.step("REMOVE FROM T VALUES(1, 99);", """
                     ts | mn | mx | weight
                    -----------------------
                     1  | 5  | 99 | -1
                     1  | 5  | 7  | 1""");
        }
    }
}
