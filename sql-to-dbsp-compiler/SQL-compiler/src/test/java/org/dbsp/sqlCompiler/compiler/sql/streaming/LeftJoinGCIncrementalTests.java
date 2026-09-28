package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.OutputPort;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPSourceBaseOperator;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/** GC of the inputs of a LEFT JOIN. */
public class LeftJoinGCIncrementalTests extends StreamingTestBase {
    /** The table that the data of {@code port} comes from, following the first inputs. */
    static String sourceTable(OutputPort port) {
        DBSPOperator operator = port.node();
        while (!operator.is(DBSPSourceBaseOperator.class))
            operator = operator.inputs.get(0).node();
        return operator.to(DBSPSourceBaseOperator.class).tableName.toString();
    }

    /** The tables whose data is retained by a retain-keys operator, in circuit order. */
    static List<String> retainedTables(CompilerCircuitStream ccs) {
        List<String> result = new ArrayList<>();
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                result.add(sourceTable(operator.left()));
            }
        });
        return result;
    }

    /** Delete the only R row with key 5 once the waterline of L is past 5.
     * @param retained  Tables whose data the program with LATENESS retains. */
    void checkRightDelete(String sql, List<String> retained) {
        String[] programs = {
                // As written
                sql,
                // Without LATENESS
                sql.replace(" LATENESS 0", "").replace(" LATENESS 100", "")
        };
        for (String program : programs) {
            var ccs = this.getCCS(program).compactAfterEachStep();
            if (program.equals(sql))
                Assert.assertEquals(retained, retainedTables(ccs));
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
                CREATE VIEW V AS SELECT L.k, L.v, R.x FROM L LEFT JOIN R ON L.k = R.k;""", List.of());
    }

    /** Both keys have LATENESS, and R lags: the right input is GC-ed below the smaller waterline.
     * Expected outputs validated with Postgres. */
    @Test
    public void rightLagging() {
        this.checkRightDelete("""
                CREATE TABLE L (k BIGINT NOT NULL LATENESS 0, v INT);
                CREATE TABLE R (k BIGINT NOT NULL LATENESS 100, x INT);
                CREATE VIEW V AS SELECT L.k, L.v, R.x FROM L LEFT JOIN R ON L.k = R.k;""", List.of("l", "r"));
    }
}
