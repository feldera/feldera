package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

/** Waterlines of functions applied to columns with LATENESS. */
public class MonotoneFunctionsIncrementalTests extends StreamingTestBase {
    /** ROUND(x, g) is not monotone in the digits g.  The program runs with and without LATENESS;
     * no input row is late.  Expected outputs validated with Postgres. */
    @Test
    public void roundDigitsWithLateness() {
        String sql = """
                CREATE TABLE T (g INT NOT NULL LATENESS 6, x DECIMAL(10, 2) NOT NULL LATENESS 0);
                CREATE VIEW V AS SELECT COUNT(*) AS c FROM T GROUP BY ROUND(x, g);""";
        String[] programs = {
                // As written
                sql,
                // Without LATENESS
                sql.replace(" LATENESS 6", "").replace(" LATENESS 0", "")
        };
        for (String program : programs) {
            var ccs = this.getCCS(program).compactAfterEachStep();
            ccs.step("INSERT INTO T VALUES(6, 1.60);", """
                     c | weight
                    ------------
                     1 | 1""");
            // The waterline of ROUND(x, g) would be ROUND(1.60, 0) = 2, above the group 1.60
            ccs.step("INSERT INTO T VALUES(6, 1.60);", """
                     c | weight
                    ------------
                     1 | -1
                     2 | 1""");
            ccs.step("INSERT INTO T VALUES(6, 1.60);", """
                     c | weight
                    ------------
                     2 | -1
                     3 | 1""");
        }
    }

    /** Checks the number of retain-keys operators in the circuit of {@code sql}. */
    void expectRetainKeys(String sql, int expected) {
        CompilerCircuit cc = this.getCC(sql);
        cc.visit(new CircuitVisitor(cc.compiler) {
            int retainKeys = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.retainKeys++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(sql, expected, this.retainKeys);
            }
        });
    }

    /** TRUNCATE(x, g) is not monotone in the digits g; a negative x shows it.  The program runs
     * with and without LATENESS; no input row is late.  Expected outputs validated with Postgres. */
    @Test
    public void truncateDigitsWithLateness() {
        String sql = """
                CREATE TABLE T (g INT NOT NULL LATENESS 6, x DECIMAL(10, 2) NOT NULL LATENESS 0);
                CREATE VIEW V AS SELECT COUNT(*) AS c FROM T GROUP BY TRUNCATE(x, g);""";
        String[] programs = {
                // As written
                sql,
                // Without LATENESS
                sql.replace(" LATENESS 6", "").replace(" LATENESS 0", "")
        };
        for (String program : programs) {
            var ccs = this.getCCS(program).compactAfterEachStep();
            ccs.step("INSERT INTO T VALUES(6, -1.60);", """
                     c | weight
                    ------------
                     1 | 1""");
            // The waterline of TRUNCATE(x, g) would be TRUNCATE(-1.60, 0) = -1, above the group -1.60
            ccs.step("INSERT INTO T VALUES(6, -1.60);", """
                     c | weight
                    ------------
                     1 | -1
                     2 | 1""");
        }
    }

    /** ROUND and TRUNCATE with constant digits keep the waterline of their value; the GROUP BY
     * integral has one retain-keys operator. */
    @Test
    public void roundWithConstantDigits() {
        String[][] cases = {
                { "x DECIMAL(10, 2)", "ROUND(x, 1)" },
                { "x DECIMAL(10, 2)", "ROUND(x, CAST(1.4 AS INTEGER))" },
                { "x DECIMAL(10, 2)", "ROUND(x)" },
                { "x DECIMAL(10, 2)", "TRUNCATE(x, 1)" },
                { "x DOUBLE", "ROUND(x, 2)" },
        };
        for (String[] c : cases) {
            this.expectRetainKeys("""
                    CREATE TABLE T (COLUMN NOT NULL LATENESS 0);
                    CREATE VIEW V AS SELECT COUNT(*) AS c FROM T GROUP BY GROUPING;"""
                    .replace("COLUMN", c[0])
                    .replace("GROUPING", c[1]), 1);
        }
    }

    /** CAST(ts AS TIME) keeps the time of day, which wraps at midnight.  The program runs with
     * and without LATENESS; no input row is late.  Expected outputs validated with Postgres. */
    @Test
    public void castToTimeWithLateness() {
        String sql = """
                CREATE TABLE T (ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR);
                CREATE VIEW V AS SELECT COUNT(*) AS c FROM T GROUP BY CAST(ts AS TIME);""";
        String[] programs = {
                // As written
                sql,
                // Without LATENESS
                sql.replace(" LATENESS INTERVAL 1 HOUR", "")
        };
        for (String program : programs) {
            var ccs = this.getCCS(program).compactAfterEachStep();
            ccs.step("INSERT INTO T VALUES('2024-01-01 10:00:00');", """
                     c | weight
                    ------------
                     1 | 1""");
            ccs.step("INSERT INTO T VALUES('2024-01-01 20:00:00');", """
                     c | weight
                    ------------
                     1 | 1""");
            ccs.step("INSERT INTO T VALUES('2024-01-02 00:30:00');", """
                     c | weight
                    ------------
                     1 | 1""");
            // Same time of day as the first row, on the next day; not late
            ccs.step("INSERT INTO T VALUES('2024-01-02 10:00:00');", """
                     c | weight
                    ------------
                     1 | -1
                     2 | 1""");
        }
    }

    /** A cast to DATE preserves order and keeps the waterline; a cast to TIME does not. */
    @Test
    public void castWaterlines() {
        String sql = """
                CREATE TABLE T (ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR);
                CREATE VIEW V AS SELECT COUNT(*) AS c FROM T GROUP BY CAST(ts AS TYPE);""";
        this.expectRetainKeys(sql.replace("TYPE", "DATE"), 1);
        this.expectRetainKeys(sql.replace("TYPE", "TIME"), 0);
    }
}
