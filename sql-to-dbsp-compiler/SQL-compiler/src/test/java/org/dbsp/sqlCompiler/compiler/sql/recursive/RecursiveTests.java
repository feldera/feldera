package org.dbsp.sqlCompiler.compiler.sql.recursive;

import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

/** Tests with recursive queries */
public class RecursiveTests extends BaseSQLTests {
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
        // Non-incremental test
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
    public void typeMismatchTest() {
        // Declared type does not match
        String sql = """
                CREATE RECURSIVE VIEW V(v INT);
                CREATE VIEW V AS SELECT CAST(V.v AS VARCHAR) FROM V UNION SELECT '1';""";
        this.statementsFailingInCompilation(sql, "does not match the declared type");

        // Declared column name does not match
        sql = """
                CREATE RECURSIVE VIEW V(v INT);
                CREATE VIEW V (v0) AS SELECT DISTINCT v FROM V UNION SELECT 1;""";
        this.statementsFailingInCompilation(sql, "does not match the declared type");

        // Declared recursive view not used anywhere
        sql = """
                CREATE RECURSIVE VIEW V(v INT);""";
        this.shouldWarn(sql, "Unused view declaration");
    }

    @Test
    public void unsupportedRecursive() {
        String sql = """
                CREATE RECURSIVE VIEW V(v BIGINT NOT NULL);
                CREATE TABLE T(v BIGINT);
                CREATE LOCAL VIEW W AS SELECT * FROM V UNION SELECT * FROM T;
                CREATE VIEW V AS SELECT COUNT(*) OVER (ORDER BY v) v FROM W;""";
        this.statementsFailingInCompilation(sql, "Operator not supported in a recursive query");
    }
}
