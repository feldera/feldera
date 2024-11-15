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
    public void illegalRecursiveTests() {
        String sql = """
                CREATE TABLE E(x int, y int);
                CREATE RECURSIVE VIEW CLOSURE(x int, y int);
                CREATE VIEW STEP AS
                SELECT E.x, CLOSURE.y FROM
                E JOIN CLOSURE ON e.y = CLOSURE.x;
                CREATE MATERIALIZED VIEW CLOSURE AS (SELECT * FROM E) UNION (SELECT * FROM STEP);
                """;
        this.statementsFailingInCompilation(sql,
                "View 'step' must be declared either as LOCAL or as RECURSIVE");

        sql = """
                CREATE TABLE E(x int, y int);
                CREATE RECURSIVE VIEW CLOSURE(x int, y int);
                CREATE MATERIALIZED VIEW CLOSURE AS (SELECT * FROM E) UNION
                    (SELECT lag(x) OVER (PARTITION BY x ORDER BY y), x FROM CLOSURE);
                """;
        this.statementsFailingInCompilation(sql,
                "Unsupported operation 'LAG' in recursive code");
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

        // Recursive view is not recursive
        sql = """
                CREATE RECURSIVE VIEW V(v INT NOT NULL);
                CREATE VIEW V AS SELECT 1 AS v;""";
        this.shouldWarn(sql, "is declared recursive, but is not used in any recursive computation");
    }

    @Test
    public void unsupportedRecursive() {
        String sql = """
                CREATE RECURSIVE VIEW V(v BIGINT NOT NULL);
                CREATE TABLE T(v BIGINT);
                CREATE LOCAL VIEW W AS SELECT * FROM V UNION SELECT * FROM T;
                CREATE VIEW V AS SELECT COUNT(*) OVER (ORDER BY v) v FROM W;""";
        this.statementsFailingInCompilation(sql, "Unsupported operation 'OVER' in recursive code");
    }
}
