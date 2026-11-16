package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Test;

/** Quantified comparisons ({@code SOME}, {@code ANY}, {@code ALL}) and the {@code UNIQUE} predicate
 * over sub-queries and VALUES lists.  Expected outputs of the quantified comparisons validated with
 * Postgres; Postgres has no {@code UNIQUE}, so its outputs were validated with the equivalent
 * {@code NOT EXISTS (... GROUP BY ... HAVING COUNT(*) > 1)} over the rows without NULL columns. */
public class QuantifiedSubQueryTests extends ScottBaseTests {
    /** {@code SOME} and its synonym {@code ANY}: the comparison is NULL when no row satisfies it
     * and the sub-query or the VALUES list contains a NULL. */
    @Test
    public void testSome() {
        this.qst("""
                SELECT empno, sal <= SOME (SELECT comm FROM emp) AS x FROM emp;
                 EMPNO | X
                -------+------
                  7369 | true
                  7499 |
                  7521 | true
                  7566 |
                  7654 | true
                  7698 |
                  7782 |
                  7788 |
                  7839 |
                  7844 |
                  7876 | true
                  7900 | true
                  7902 |
                  7934 | true
                (14 rows)

                SELECT ename FROM emp WHERE sal > ANY (SELECT sal FROM emp WHERE deptno = 10);
                 ENAME
                --------
                 ALLEN
                 BLAKE
                 CLARK
                 FORD
                 JONES
                 KING
                 SCOTT
                 TURNER
                (8 rows)

                SELECT empno, empno <> SOME (VALUES 7499, NULL) AS x FROM emp WHERE deptno = 30;
                 EMPNO | X
                -------+------
                  7499 |
                  7521 | true
                  7654 | true
                  7698 | true
                  7844 | true
                  7900 | true
                (6 rows)""");
    }

    /** {@code ALL}: the comparison is NULL when every non-NULL row satisfies it and the sub-query
     * contains a NULL; {@code <> ALL} over a sub-query with a NULL never holds. */
    @Test
    public void testAll() {
        this.qst("""
                SELECT empno, sal > ALL (SELECT comm FROM emp) AS x FROM emp;
                 EMPNO | X
                -------+-------
                  7369 | false
                  7499 |
                  7521 | false
                  7566 |
                  7654 | false
                  7698 |
                  7782 |
                  7788 |
                  7839 |
                  7844 |
                  7876 | false
                  7900 | false
                  7902 |
                  7934 | false
                (14 rows)

                SELECT ename FROM emp WHERE sal >= ALL (SELECT sal FROM emp);
                 ENAME
                -------
                 KING
                (1 row)

                SELECT ename FROM emp WHERE empno <> ALL (SELECT mgr FROM emp);
                 ENAME
                -------
                (0 rows)

                SELECT ename FROM emp WHERE empno <> ALL (SELECT mgr FROM emp WHERE mgr IS NOT NULL);
                 ENAME
                --------
                 ADAMS
                 ALLEN
                 JAMES
                 MARTIN
                 MILLER
                 SMITH
                 TURNER
                 WARD
                (8 rows)""");
    }

    /** Over an empty sub-query {@code ALL} is true and {@code SOME} is false, even for a NULL left
     * operand. */
    @Test
    public void testEmptySubQuery() {
        this.qst("""
                SELECT empno,
                  comm < ALL (SELECT comm FROM emp WHERE comm = 800) AS a,
                  comm < SOME (SELECT comm FROM emp WHERE comm = 800) AS s
                FROM emp WHERE deptno = 30;
                 EMPNO | A    | S
                -------+------+-------
                  7499 | true | false
                  7521 | true | false
                  7654 | true | false
                  7698 | true | false
                  7844 | true | false
                  7900 | true | false
                (6 rows)""");
    }

    /** Correlated quantified comparisons, in a WHERE clause and in a SELECT list; department 40 has
     * no employees. */
    @Test
    public void testCorrelated() {
        this.qst("""
                SELECT ename, deptno FROM emp e
                WHERE sal >= ALL (SELECT sal FROM emp e2 WHERE e2.deptno = e.deptno);
                 ENAME | DEPTNO
                -------+--------
                 BLAKE |     30
                 FORD  |     20
                 KING  |     10
                 SCOTT |     20
                (4 rows)

                SELECT dname, 3000 < SOME (SELECT sal FROM emp WHERE emp.deptno = dept.deptno) AS x FROM dept;
                 DNAME      | X
                ------------+-------
                 ACCOUNTING | true
                 OPERATIONS | false
                 RESEARCH   | false
                 SALES      | false
                (4 rows)""");
    }

    /** Quantified comparisons with a VALUES list. */
    @Test
    public void testValues() {
        this.qst("""
                SELECT 10 <= ANY (VALUES 10, 20, 30) AS a, 10 <= SOME (VALUES 10, 20, 30) AS s,
                  10 <= ALL (VALUES 10, 20, 30) AS l, 20 <= ALL (VALUES 10, 20, 30) AS l2;
                 A    | S    | L    | L2
                ------+------+------+-------
                 true | true | true | false
                (1 row)""");
    }

    /** Quantified comparisons with an array are rejected */
    @Test
    public void testArray() {
        // The newline checks that nothing follows the URL
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT empno = ANY (ARRAY[7369, 7499]) AS x FROM emp;", """
                error: Not supported: Comparisons with SOME, ANY, or ALL over an array are not supported.
                See https://docs.feldera.com/sql/unsupported-operations#quantified-comparisons-with-arrays
                """);
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT ename FROM emp WHERE empno > ALL (ARRAY[7369, 7499]);",
                "Not supported: Comparisons with SOME, ANY, or ALL over an array are not supported.");
    }

    /** {@code <=>} and {@code IS [NOT] DISTINCT FROM} cannot be quantified; the parser would
     * read {@code SOME (query)} as a call of the {@code SOME} aggregate function */
    @Test
    public void testDistinctQuantified() {
        this.statementsFailingInCompilation("CREATE VIEW V AS SELECT empno <=> SOME (SELECT mgr FROM emp) AS x FROM emp;", """
                error: Not supported: '<=>' cannot be combined with 'SOME'; consider using 'EXISTS' instead.
                See https://docs.feldera.com/sql/comparisons#any
                """);
        this.statementsFailingInCompilation(
                "CREATE VIEW V AS SELECT empno IS NOT DISTINCT FROM ALL (SELECT mgr FROM emp) AS x FROM emp;",
                "'IS NOT DISTINCT FROM' cannot be combined with 'ALL'; consider using 'NOT EXISTS' instead.");
        this.statementsFailingInCompilation(
                "CREATE VIEW V AS SELECT empno IS DISTINCT FROM SOME (SELECT mgr FROM emp) AS x FROM emp;",
                "'IS DISTINCT FROM' cannot be combined with 'SOME'; consider using 'EXISTS' instead.");
        // With a BOOLEAN query this used to compile as an aggregation
        this.statementsFailingInCompilation(
                "CREATE VIEW V AS SELECT TRUE <=> SOME (SELECT comm > 1000 FROM emp) AS x FROM emp;",
                "'<=>' cannot be combined with 'SOME'; consider using 'EXISTS' instead.");
    }

    /** {@code UNIQUE} is true unless two rows are equal and have no NULL column. */
    @Test
    public void testUnique() {
        this.qst("""
                SELECT deptno, UNIQUE (SELECT comm FROM emp) AS u FROM dept;
                 DEPTNO | U
                --------+------
                     10 | true
                     20 | true
                     30 | true
                     40 | true
                (4 rows)

                SELECT dname, UNIQUE (SELECT job FROM emp WHERE emp.deptno = dept.deptno) AS u FROM dept;
                 DNAME      | U
                ------------+-------
                 ACCOUNTING | true
                 OPERATIONS | true
                 RESEARCH   | false
                 SALES      | false
                (4 rows)

                SELECT dname FROM dept WHERE NOT UNIQUE (SELECT job FROM emp WHERE emp.deptno = dept.deptno);
                 DNAME
                ----------
                 RESEARCH
                 SALES
                (2 rows)

                SELECT UNIQUE (SELECT mgr, deptno FROM emp) AS u;
                 U
                -------
                 false
                (1 row)""");
    }
}
