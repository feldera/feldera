package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamJoinOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

public class Regression3Tests extends SqlIoTest {
    @Test
    public void withSuggestion() {
        this.statementsFailingInCompilation("WITH V AS (SELECT 1) SELECT * FROM V;",
                "Raw 'SELECT' statements are not supported; did you forget to CREATE VIEW?");
    }

    @Test
    public void issue5806() {
        // Temporal filters and joins can be swapped by Calcite optimizer
        var cc = this.getCC("""
                CREATE TABLE T(x INT, ts TIMESTAMP);
                CREATE TABLE S(x INT);
                CREATE VIEW W AS WITH V AS (SELECT * FROM T JOIN S ON T.x = S.x)
                SELECT * FROM V WHERE ts > NOW() - INTERVAL 1 DAY;""");
        cc.visit(new CircuitVisitor(cc.compiler) {
            boolean waterlineFound = false;
            boolean joinFound = false;

            @Override
            public void postorder(DBSPWaterlineOperator window) {
                // If the optimization works the waterline will be before the join
                Assert.assertFalse(this.joinFound);
                waterlineFound = true;
            }

            @Override
            public void postorder(DBSPStreamJoinOperator window) {
                // If the optimization works the waterline will be before the join
                Assert.assertTrue(waterlineFound);
                joinFound = true;
            }
        });
    }

    @Test
    public void issue5806a() {
        // Temporal filters and joins can be swapped by Calcite optimizer; test for LEFT JOIN
        var cc = this.getCC("""
                CREATE TABLE T(x INT, ts TIMESTAMP);
                CREATE TABLE S(x INT);
                CREATE VIEW W AS WITH V AS (SELECT * FROM T LEFT JOIN S ON T.x = S.x)
                SELECT * FROM V WHERE ts > TIMESTAMP '2020-01-01 10:00:00';""");
        cc.visit(new CircuitVisitor(cc.compiler) {
            boolean waterlineFound = false;
            boolean joinFound = false;

            @Override
            public void postorder(DBSPWaterlineOperator window) {
                // If the optimization works the waterline will be before the join
                Assert.assertFalse(this.joinFound);
                waterlineFound = true;
            }

            @Override
            public void postorder(DBSPStreamJoinOperator window) {
                // If the optimization works the waterline will be before the join
                Assert.assertTrue(waterlineFound);
                joinFound = true;
            }
        });
    }

    @Test
    public void testReplace() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT, y INT);
                CREATE VIEW V AS SELECT * REPLACE(x+y AS y) FROM T;""");
        ccs.stepWeightOne("INSERT INTO T VALUES(1, 2)", """
                 x | y
                -------
                 1 | 3""");
    }
}
