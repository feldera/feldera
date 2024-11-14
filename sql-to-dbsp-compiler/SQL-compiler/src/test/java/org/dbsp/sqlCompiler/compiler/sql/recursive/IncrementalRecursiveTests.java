package org.dbsp.sqlCompiler.compiler.sql.recursive;

import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.Passes;
import org.dbsp.util.Logger;
import org.junit.Assert;
import org.junit.Test;

/** Tests with recursive queries and incremental compilation */
public class IncrementalRecursiveTests extends BaseSQLTests {
    @Override
    public DBSPCompiler testCompiler() {
        CompilerOptions options = this.testOptions(true, true);
        return new DBSPCompiler(options);
    }

    @Test
    public void testRecursive() {
        String sql = """
                CREATE RECURSIVE VIEW V(v INT);
                CREATE VIEW V AS SELECT v FROM V UNION SELECT 1;""";
        var ccs = this.getCCS(sql);
        ccs.step("", """
                 v | weight
                ------------
                 1 | 1""");
        this.addRustTestCase(ccs);
        CircuitVisitor visitor = new CircuitVisitor(new StderrErrorReporter()) {
            int recursive = 0;
            @Override
            public void postorder(DBSPNestedOperator operator) {
                this.recursive++;
            }
            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.recursive);
            }
        };
        visitor.apply(ccs.circuit);
    }

    @Test
    public void testRecursive2() {
        String sql = """
                CREATE RECURSIVE VIEW V(v INT);
                CREATE TABLE T(v INT);
                CREATE VIEW V AS SELECT v FROM V UNION SELECT * FROM T;""";
        var ccs = this.getCCS(sql);
        ccs.step("", """
                 v | weight
                ------------""");
        ccs.step("INSERT INTO T VALUES(1)", """
                 v | weight
                ------------
                 1 | 1""");
        ccs.step("INSERT INTO T VALUES(2)", """
                 v | weight
                ------------
                 2 | 1""");
        this.addRustTestCase(ccs);
    }

    @Test
    public void testRecursiveInMiddle() {
        // Circuit with a recursive component sandwiched between two
        // non-recursive components.
        String sql = """
                CREATE TABLE T(v INT);
                CREATE LOCAL VIEW X AS SELECT v/2 FROM T;
                CREATE RECURSIVE VIEW V(v INT);
                CREATE LOCAL VIEW V AS SELECT v FROM V UNION SELECT * FROM X;
                CREATE VIEW O AS SELECT v+1 FROM V;""";
        var ccs = this.getCCS(sql);
        ccs.step("", """
                 v | weight
                ------------""");
        ccs.step("INSERT INTO T VALUES(1)", """
                 v | weight
                ------------
                 1 | 1""");
        ccs.step("INSERT INTO T VALUES(2)", """
                 v | weight
                ------------
                 2 | 1""");
        this.addRustTestCase(ccs);
    }

    @Test
    public void transitiveClosure() {
        String sql = """
                CREATE TABLE E(x int, y int);
                CREATE RECURSIVE VIEW CLOSURE(x int, y int);
                CREATE LOCAL VIEW STEP AS
                SELECT E.x, CLOSURE.y FROM
                E JOIN CLOSURE ON e.y = CLOSURE.x;
                CREATE VIEW CLOSURE AS (SELECT * FROM E) UNION (SELECT * FROM STEP);
                """;
        var ccs = this.getCCS(sql);
        ccs.step("", """
                 x | y | weight
                ----------------""");
        ccs.step("INSERT INTO E VALUES(0, 1);", """
                 x | y | weight
                ----------------
                 0 | 1 | 1""");
        ccs.step("INSERT INTO E VALUES(1, 2);", """
                x | y | weight
                ---------------
                1 | 2 | 1
                0 | 2 | 1""");
        ccs.step("REMOVE FROM E VALUES(0, 1);", """
                x | y | weight
                ---------------
                0 | 1 | -1
                0 | 2 | -1""");
        this.addRustTestCase(ccs);
    }
}
