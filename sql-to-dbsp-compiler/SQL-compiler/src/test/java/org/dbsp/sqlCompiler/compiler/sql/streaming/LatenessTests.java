package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainNValuesOperator;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

/** Tests for data with LATENESS */
public class LatenessTests  extends StreamingTestBase {
    @Test
    public void testMix() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts INT, z VARCHAR, x INT LATENESS 2);
                CREATE VIEW V AS SELECT ts, MIN(x), MAX(x), ARG_MAX(z, x), ARG_MIN(z, x), SUM(x) FROM t GROUP BY ts;""")
                .compactAfterEachStep().withStringTrim();
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int retain = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
                this.retain++;
            }

            @Override
            public void endVisit() {
                // MIN and MAX read the same input but keep opposite ends of the range below the
                // waterline, and so do ARG_MIN and ARG_MAX; each aggregate keeps its own retain.
                Assert.assertEquals(4, this.retain);
            }
        });
        ccs.step("INSERT INTO T VALUES (1, 'a', 20), (1, 'b', 21);", """
                 ts | min | max | argmax | argmin | sum | weight
                -----------------------------------------------
                  1 |  20 |  21 | b      | a      |  41 | 1""");
        ccs.step("REMOVE FROM T VALUES (1, 'b', 21);", """
                 ts | min | max | argmax | argmin | sum | weight
                -----------------------------------------------
                  1 |  20 |  21 | b      | a      |  41 | -1
                  1 |  20 |  20 | a      | a      |  20 | 1""");
        ccs.step("INSERT INTO T VALUES (1, 'c', 22);", """
                 ts | min | max | argmax | argmin | sum | weight
                -----------------------------------------------
                  1 |  20 |  20 | a      | a      |  20 | -1
                  1 |  20 |  22 | c      | a      |  42 | 1""");
        // Emptying the table removes the group, so the view has no rows left
        ccs.step("REMOVE FROM T VALUES (1, 'a', 20), (1, 'c', 22);", """
                 ts | min | max | argmax | argmin | sum | weight
                -----------------------------------------------
                  1 |  20 |  22 | c      | a      |  42 | -1""");
    }

    @Test
    public void testMixNoGroup() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts INT, z VARCHAR, x INT LATENESS 2);
                CREATE VIEW V AS SELECT MIN(x), MAX(x), ARG_MAX(z, x), ARG_MIN(z, x), SUM(x) FROM t;""")
                .compactAfterEachStep().withStringTrim();
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int retain = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
                this.retain++;
            }

            @Override
            public void endVisit() {
                // MIN and MAX read the same input but keep opposite ends of the range below the
                // waterline, and so do ARG_MIN and ARG_MAX; each aggregate keeps its own retain.
                Assert.assertEquals(4, this.retain);
            }
        });
        // The aggregate of the empty table is a row of nulls
        ccs.step("", """
                 min | max | argmax | argmin | sum | weight
                -------------------------------------------
                NULL |NULL |NULL    |NULL    |NULL | 1""");
        ccs.step("INSERT INTO T VALUES (1, 'a', 20), (1, 'b', 21);", """
                 min | max | argmax | argmin | sum | weight
                -------------------------------------------
                NULL |NULL |NULL    |NULL    |NULL | -1
                  20 |  21 | b      | a      |  41 | 1""");
        ccs.step("REMOVE FROM T VALUES (1, 'b', 21);", """
                 min | max | argmax | argmin | sum | weight
                -------------------------------------------
                  20 |  21 | b      | a      |  41 | -1
                  20 |  20 | a      | a      |  20 | 1""");
        ccs.step("INSERT INTO T VALUES (1, 'c', 22);", """
                 min | max | argmax | argmin | sum | weight
                -------------------------------------------
                  20 |  20 | a      | a      |  20 | -1
                  20 |  22 | c      | a      |  42 | 1""");
        ccs.step("REMOVE FROM T VALUES (1, 'a', 20), (1, 'c', 22);", """
                 min | max | argmax | argmin | sum | weight
                -------------------------------------------
                  20 |  22 | c      | a      |  42 | -1
                NULL |NULL |NULL    |NULL    |NULL | 1""");
    }

    @Test
    public void reverseLateness() {
        // Generates GC even if the field compared does NOT have lateness
        var ccs = this.getCCS("""
                CREATE TABLE T(ts INT LATENESS 2, z VARCHAR, x INT);
                CREATE VIEW V AS SELECT ARG_MIN(ts, x) FROM T;""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int retain = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
                this.retain++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.retain);
            }
        });
    }

    @Test
    public void latenessBoth() {
        var ccs = this.getCCS("""
                CREATE TABLE T(ts INT LATENESS 2, z VARCHAR, x INT LATENESS 3);
                CREATE VIEW V AS SELECT ARG_MIN(ts, x) FROM T;""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int retain = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
                this.retain++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.retain);
            }
        });
    }
}
