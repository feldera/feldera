package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.operator.DBSPStarJoinFilterMapOperator;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

/** Programs that disable star joins.  The tests of a class share one Rust crate, which is written
 * with the compiler of one of the tests, so a program that disables star joins cannot share a
 * class with programs that have star joins. */
public class StarJoinOptOutIncrementalTests extends StreamingTestBase {
    @Test
    public void starJoinFlatmapOptOutTest() {
        // Test that we can inhibit the use of star joins using options
        // This test may eventually be removed
        var ccs = this.getCCS("""
                SET FELDERA_AVOID_STAR_JOINS = ON;
                
                CREATE TABLE T(x INT, y INT);
                CREATE VIEW V AS SELECT
                y,
                MIN(x),
                MAX(x),
                STDDEV(x),
                ARG_MAX(y, x)
                FROM T GROUP BY y
                HAVING y > 1;""");
        ccs.step("INSERT INTO T VALUES(0, 0), (1, 2), (2, 2)", """
                 y | min | max | stddev | arg_max | weight
                -------------------------------------------
                 2 |   1 |   2 |      1 |       2 | 1""");
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            int joins = 0;

            @Override
            public void postorder(DBSPStarJoinFilterMapOperator operator) {
                this.joins++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(0, this.joins);
            }
        });
    }
}
