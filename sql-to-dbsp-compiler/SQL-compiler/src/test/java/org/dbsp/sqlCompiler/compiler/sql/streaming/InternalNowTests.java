package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.operator.DBSPWaterlineOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPWindowOperator;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

/** Tests that exercise the internal implementation of NOW as a source operator */
public class InternalNowTests extends StreamingTestBase {
    @Test
    public void testNow() {
        String sql = """
                CREATE VIEW V AS SELECT 1, NOW() > TIMESTAMP '2022-12-12 00:00:00';""";
           CompilerCircuitStream ccs = this.getCCS(sql);
        ccs.step("",
                """
                         c | compare | weight
                        ----------------------
                         1 | true    | 1""");
    }

    @Test
    public void testNow8() {
        // now() used in WHERE with complex monotone function
        String sql = """
                CREATE TABLE transactions (
                    usr  VARCHAR,
                    tim  TIMESTAMP
                );

                CREATE VIEW window_computation AS
                SELECT
                  usr,
                  COUNT(*) AS transaction_count_by_user
                FROM transactions
                WHERE tim >= (now() - INTERVAL 1 DAY)
                GROUP BY usr;""";
        CompilerCircuit cc = this.getCC(sql);
        CircuitVisitor visitor = new CircuitVisitor(cc.compiler) {
            int window = 0;
            int waterline = 0;

            @Override
            public void postorder(DBSPWindowOperator operator) {
                this.window++;
            }

            @Override
            public void postorder(DBSPWaterlineOperator operator) {
                this.waterline++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.window);
                Assert.assertEquals(1, this.waterline);
            }
        };
        cc.visit(visitor);
    }
}
