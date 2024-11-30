package org.dbsp.sqlCompiler.compiler.sql.recursive;

import org.dbsp.sqlCompiler.circuit.operator.DBSPNestedOperator;
import org.dbsp.sqlCompiler.compiler.CompilerOptions;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.StderrErrorReporter;
import org.dbsp.sqlCompiler.compiler.sql.tools.BaseSQLTests;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
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
        CircuitVisitor visitor = new CircuitVisitor(ccs.compiler) {
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
                CREATE TABLE EDGES(x int, y int);
                CREATE RECURSIVE VIEW CLOSURE(x int, y int);
                CREATE LOCAL VIEW STEP AS
                SELECT EDGES.x, CLOSURE.y FROM
                EDGES JOIN CLOSURE ON EDGES.y = CLOSURE.x;
                CREATE MATERIALIZED VIEW CLOSURE AS (SELECT * FROM EDGES) UNION (SELECT * FROM STEP);
                """;
        var ccs = this.getCCS(sql);
        ccs.step("", """
                 x | y | weight
                ----------------""");
        ccs.step("INSERT INTO EDGES VALUES(0, 1);", """
                 x | y | weight
                ----------------
                 0 | 1 | 1""");
        ccs.step("INSERT INTO EDGES VALUES(1, 2);", """
                x | y | weight
                ---------------
                1 | 2 | 1
                0 | 2 | 1""");
        ccs.step("REMOVE FROM EDGES VALUES(0, 1);", """
                x | y | weight
                ---------------
                0 | 1 | -1
                0 | 2 | -1""");
        this.addRustTestCase(ccs);
    }

    @Test
    public void factorial() {
        // Three tests adapted from https://www.geeksforgeeks.org/postgresql-create-recursive-views/
        String sql = """
                CREATE RECURSIVE VIEW fact(n int, factorial int);
                CREATE VIEW fact AS
                   SELECT 1 as n, 5 as factorial
                   UNION ALL SELECT n+1, factorial*n FROM fact WHERE n < 5;""";
        var ccs = this.getCCS(sql);
        ccs.step("", """
                 n | factorial | weight
                ---+--------------------
                 1 |         5 | 1
                 2 |         5 | 1
                 3 |        10 | 1
                 4 |        30 | 1
                 5 |       120 | 1""");
        this.addRustTestCase(ccs);
    }

    @Test
    public void testSequence() {
        String sql = """
                CREATE RECURSIVE VIEW tens(n int);
                CREATE VIEW tens AS
                    SELECT 1 as n
                 UNION ALL
                   SELECT n+1 FROM tens WHERE n < 10;""";
        var ccs = this.getCCS(sql);
        ccs.step("", """
                 n  | weight
                -------------
                  1 | 1
                  2 | 1
                  3 | 1
                  4 | 1
                  5 | 1
                  6 | 1
                  7 | 1
                  8 | 1
                  9 | 1
                 10 | 1""");
        this.addRustTestCase(ccs);
    }

    @Test
    public void testHierarchy() {
        String sql = """
                CREATE TABLE emp (
                  emp_id INT NOT NULL PRIMARY KEY,
                  emp_name VARCHAR,
                  manager_id INT
                );
                
                CREATE RECURSIVE VIEW subordinates(emp_id int NOT NULL, manager_id int, emp_name varchar, level int);
                CREATE VIEW subordinates AS
                  SELECT emp_id, manager_id, emp_name, 0 AS level
                  FROM emp
                  WHERE manager_id IS NULL
                  UNION ALL
                  SELECT e.emp_id, e.manager_id, e.emp_name, s.level + 1
                  FROM emp e
                  INNER JOIN subordinates s ON s.emp_id = e.manager_id;""";
        var ccs = this.getCCS(sql);
        ccs.step("""
                INSERT INTO emp
                VALUES
                (1, 'Onkar', NULL),
                (2, 'Isaac', 1),
                (3, 'Jack', 1),
                (4, 'Aditya', 1),
                (5, 'Albert', 1),
                (6, 'Alex', 2),
                (7, 'Brian', 2),
                (8, 'Harry', 3),
                (9, 'Paul', 3),
                (10, 'Kunal', 4),
                (11, 'Pranav', 5)""", """
                 emp_id | manager_id | emp_name | level | weight
                -------------------------------------------------
                      1 |            | Onkar|         0 | 1
                      2 |          1 | Isaac|         1 | 1
                      3 |          1 | Jack|          1 | 1
                      4 |          1 | Aditya|        1 | 1
                      5 |          1 | Albert|        1 | 1
                      6 |          2 | Alex|          2 | 1
                      7 |          2 | Brian|         2 | 1
                      8 |          3 | Harry|         2 | 1
                      9 |          3 | Paul|          2 | 1
                     10 |          4 | Kunal|         2 | 1
                     11 |          5 | Pranav|        2 | 1""");
        this.addRustTestCase(ccs);
    }
}
