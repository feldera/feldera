package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.quidem.ScottBaseTests;
import org.junit.Test;

/** End-to-end tests for SQL set operations: UNION, UNION ALL, INTERSECT, INTERSECT ALL, EXCEPT.
 * Some tests are from set-op.iq */
public class SetOpTests extends ScottBaseTests {
    @Test
    public void calciteIntersect() {
        this.qst("""
             -- Intersect all
            select * from
            (select x, y from (values (1, 'a'), (1, 'a'), (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y))
            intersect all
            (select x, y from (values (1, 'a'), (1, 'a'), (2, 'c'), (4, 'x')) as t2(x, y));
            +---+---+
            | X | Y |
            +---+---+
            | 1 | a |
            | 1 | a |
            +---+---+
            (2 rows)

            -- Intersect
            select * from
            (select x, y from (values (1, 'a'), (1, 'a'), (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y))
            intersect
            (select x, y from (values (1, 'a'), (1, 'a'), (2, 'c'), (4, 'x')) as t2(x, y));
            +---+---+
            | X | Y |
            +---+---+
            | 1 | a |
            +---+---+
            (1 row)

            -- Intersect all with null value rows
            select * from
            (select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),
            (cast(NULL as int), cast(NULL as varchar(1))), (cast(NULL as int), cast(NULL as varchar(1)))) as t(x, y))
            intersect all
            (select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),
            (cast(NULL as int), cast(NULL as varchar(1)))) as t2(x, y));
            +----+----+
            | X  | Y  |
            +----+----+
            |NULL|NULL|
            |NULL|NULL|
            +----+----+
            (2 rows)

            -- Intersect with null value rows
            select * from
            (select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),
            (cast(NULL as int), cast(NULL as varchar(1))), (cast(NULL as int), cast(NULL as varchar(1)))) as t(x, y))
            intersect
            (select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),
            (cast(NULL as int), cast(NULL as varchar(1)))) as t2(x, y));
            +----+----+
            | X  | Y  |
            +----+----+
            |NULL|NULL|
            +----+----+
            (1 row)""");
    }

    @Test
    public void calciteUnionTest() {
        this.qst("""
            -- Union all
            select * from
            (select x, y from (values (1, 'a'), (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y))
            union all
            (select x, y from (values (1, 'a'), (2, 'c'), (4, 'x')) as t2(x, y));
            +---+---+
            | X | Y |
            +---+---+
            | 1 | a |
            | 1 | a |
            | 1 | a |
            | 2 | b |
            | 2 | c |
            | 3 | c |
            | 4 | x |
            +---+---+
            (7 rows)

            -- Union
            select * from
            (select x, y from (values (1, 'a'), (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y))
            union
            (select x, y from (values (1, 'a'), (2, 'c'), (4, 'x')) as t2(x, y));
            +---+---+
            | X | Y |
            +---+---+
            | 1 | a |
            | 2 | b |
            | 2 | c |
            | 3 | c |
            | 4 | x |
            +---+---+
            (5 rows)

            -- Union all with null value rows
            select * from
            (select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),
            (cast(NULL as int), cast(NULL as varchar(1))), (cast(NULL as int), cast(NULL as varchar(1)))) as t(x, y))
            union all
            (select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),
            (cast(NULL as int), cast(NULL as varchar(1)))) as t2(x, y));
            +----+----+
            | X  | Y  |
            +----+----+
            |NULL|NULL|
            |NULL|NULL|
            |NULL|NULL|
            |NULL|NULL|
            |NULL|NULL|
            +----+----+
            (5 rows)

            -- Union with null value rows
            select * from
            (select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),
            (cast(NULL as int), cast(NULL as varchar(1))), (cast(NULL as int), cast(NULL as varchar(1)))) as t(x, y))
            union
            (select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),
            (cast(NULL as int), cast(NULL as varchar(1)))) as t2(x, y));
            +----+----+
            | X  | Y  |
            +----+----+
            |NULL|NULL|
            +----+----+
            (1 row)""");
    }

    @Test
    public void calciteExcept() {
        this.qst("""
            -- Except all
            select * from
            (select x, y from (values (1, 'a'), (1, 'a'), (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y))
            except all
            (select x, y from (values (1, 'a'), (2, 'c'), (4, 'x')) as t2(x, y));
            +---+---+
            | X | Y |
            +---+---+
            | 1 | a |
            | 1 | a |
            | 2 | b |
            | 3 | c |
            +---+---+
            (4 rows)

            -- Except
            select * from
            (select x, y from (values (1, 'a'), (1, 'a'), (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y))
            except
            (select x, y from (values (1, 'a'), (2, 'c'), (4, 'x')) as t2(x, y));
            +---+---+
            | X | Y |
            +---+---+
            | 2 | b |
            | 3 | c |
            +---+---+
            (2 rows)

            -- Except all with null value rows
            select * from
            (select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),
            (cast(NULL as int), cast(NULL as varchar(1))), (cast(NULL as int), cast(NULL as varchar(1)))) as t(x, y))
            except all
            (select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),
            (cast(NULL as int), cast(NULL as varchar(1)))) as t2(x, y));
            +----+----+
            | X  | Y  |
            +----+----+
            |NULL|NULL|
            +----+----+
            (1 row)

            -- Except with null value rows
            select * from
            (select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),
            (cast(NULL as int), cast(NULL as varchar(1))), (cast(NULL as int), cast(NULL as varchar(1)))) as t(x, y))
            except
            (select x, y from (values (cast(NULL as int), cast(NULL as varchar(1))),
            (cast(NULL as int), cast(NULL as varchar(1)))) as t2(x, y));
            +---+---+
            | X | Y |
            +---+---+
            +---+---+
            (0 rows)""");
    }

    @Test
    public void calciteScottTests() {
        this.qst("""
            -- Parentheses at top level
            (select * from emp where deptno = 10
            union all
            select * from emp where job = 'CLERK')
            intersect
            select * from emp where job = 'CLERK';
            +-------+--------+-------+------+------------+---------+------+--------+
            | EMPNO | ENAME  | JOB   | MGR  | HIREDATE   | SAL     | COMM | DEPTNO |
            +-------+--------+-------+------+------------+---------+------+--------+
            |  7369 | SMITH  | CLERK | 7902 | 1980-12-17 |  800.00 |      |     20 |
            |  7876 | ADAMS  | CLERK | 7788 | 1987-05-23 | 1100.00 |      |     20 |
            |  7900 | JAMES  | CLERK | 7698 | 1981-12-03 |  950.00 |      |     30 |
            |  7934 | MILLER | CLERK | 7782 | 1982-01-23 | 1300.00 |      |     10 |
            +-------+--------+-------+------+------------+---------+------+--------+
            (4 rows)

            -- [CALCITE-6303] UNION with CTE(s) results in exception during query validation
            (SELECT 123)
            UNION
            (WITH t (col) AS (VALUES (456)) SELECT col FROM t);
            +--------+
            | EXPR$0 |
            +--------+
            |    123 |
            |    456 |
            +--------+
            (2 rows)

            -- [CALCITE-6955] PruneEmptyRules does not handle the all attribute of SetOp correctly
            select * from (values (10, 1), (30, 3), (30, 3)) as t (x, y)
            union
            select * from (values (20, 2)) as t (x, y) where x > 30;
            +----+---+
            | X  | Y |
            +----+---+
            | 10 | 1 |
            | 30 | 3 |
            +----+---+
            (2 rows)

            -- [CALCITE-6955] PruneEmptyRules does not handle the all attribute of SetOp correctly
            select * from (values (30, 3), (30, 3)) as t (x, y)
            except
            select * from (values (20, 2)) as t (x, y) where x > 30;
            +----+---+
            | X  | Y |
            +----+---+
            | 30 | 3 |
            +----+---+
            (1 row)

            -- [CALCITE-7429] Query with MINUS fails with "Unable to implement EnumerableMinus(all=[false])"
            SELECT deptno FROM dept WHERE deptno > 12
            EXCEPT
            SELECT deptno FROM emp e1 WHERE EXISTS (
                SELECT 1 FROM emp e2
                WHERE e2.comm = e1.comm);
            +--------+
            | DEPTNO |
            +--------+
            |     20 |
            |     40 |
            +--------+
            (2 rows)

            SELECT deptno FROM dept WHERE deptno > 12
            INTERSECT
            SELECT deptno FROM emp e1 WHERE EXISTS (
                SELECT 1 FROM emp e2
                WHERE e2.comm = e1.comm);
            +--------+
            | DEPTNO |
            +--------+
            |     30 |
            +--------+
            (1 row)""");
    }

    @Test
    public void testUnionBasic() {
        this.qst("""
                SELECT * FROM (VALUES (1), (2), (2)) AS T(x)
                UNION
                SELECT * FROM (VALUES (2), (3)) AS T(x);
                 x
                ---
                 1
                 2
                 3
                (3 rows)""");
    }

    @Test
    public void testUnionNull() {
        // NULL is treated as a distinct value in UNION (NOT DISTINCT FROM semantics).
        this.qst("""
                SELECT * FROM (VALUES (1), (CAST(NULL AS INT))) AS T(x)
                UNION
                SELECT * FROM (VALUES (CAST(NULL AS INT)), (2)) AS T(x);
                 x
                ------
                 1
                 NULL
                 2
                (3 rows)""");
    }

    @Test
    public void testUnionMultiColumn() {
        this.qst("""
                SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (2, 'b')) AS T(x, s)
                UNION
                SELECT * FROM (VALUES (2, 'b'), (3, 'c')) AS T(x, s);
                 x | s
                ------
                 1 | a
                 2 | b
                 3 | c
                (3 rows)""");
    }

    @Test
    public void testUnionAllBasic() {
        // Duplicates are preserved.
        this.qst("""
                SELECT * FROM (VALUES (1), (2)) AS T(x)
                UNION ALL
                SELECT * FROM (VALUES (2), (3)) AS T(x);
                 x
                ---
                 1
                 2
                 2
                 3
                (4 rows)""");
    }

    @Test
    public void testUnionAllNull() {
        // Each NULL appears independently in the multiset.
        this.qst("""
                SELECT * FROM (VALUES (CAST(NULL AS INT)), (1)) AS T(x)
                UNION ALL
                SELECT * FROM (VALUES (CAST(NULL AS INT)), (1)) AS T(x);
                 x
                ------
                 NULL
                 1
                 NULL
                 1
                (4 rows)""");
    }

    @Test
    public void testIntersectBasic() {
        this.qst("""
                SELECT * FROM (VALUES (1), (2), (2)) AS T(x)
                INTERSECT
                SELECT * FROM (VALUES (2), (2), (3)) AS T(x);
                 x
                ---
                 2
                (1 row)""");
    }

    @Test
    public void testIntersectEmpty() {
        // No common values: result is empty.
        this.qst("""
                SELECT * FROM (VALUES (1), (2)) AS T(x)
                INTERSECT
                SELECT * FROM (VALUES (3), (4)) AS T(x);
                 x
                ---
                (0 rows)""");
    }

    @Test
    public void testIntersectNull() {
        // NULL IS NOT DISTINCT FROM NULL, so NULL is kept in the intersection.
        this.qst("""
                SELECT * FROM (VALUES (CAST(NULL AS INT)), (1)) AS T(x)
                INTERSECT
                SELECT * FROM (VALUES (CAST(NULL AS INT)), (2)) AS T(x);
                 x
                ------
                 NULL
                (1 row)""");
    }

    @Test
    public void testIntersectNullExcluded() {
        // NULL only on the left side: not in the intersection.
        this.qst("""
                SELECT * FROM (VALUES (CAST(NULL AS INT)), (1)) AS T(x)
                INTERSECT
                SELECT * FROM (VALUES (2), (3)) AS T(x);
                 x
                ---
                (0 rows)""");
    }

    @Test
    public void testIntersectMultiField() {
        this.qst("""
                SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) AS T(x, y)
                INTERSECT
                SELECT * FROM (VALUES (1, 10), (2, 99), (4, 40)) AS T(x, y);
                 x | y
                -------
                 1 | 10
                (1 row)""");
    }

    @Test
    public void testIntersectAllBasic() {
        // A = {1, 2, 2}  B = {2, 2, 3}
        // Result = {2, 2}  (min(0,0)=0 for 1 and 3, min(2,2)=2 for 2)
        this.qst("""
                SELECT * FROM (VALUES (1), (2), (2)) AS T(x)
                INTERSECT ALL
                SELECT * FROM (VALUES (2), (2), (3)) AS T(x);
                 x
                ---
                 2
                 2
                (2 rows)""");
    }

    @Test
    public void testIntersectAllMinCounts() {
        // A = {1, 1, 1}  B = {1, 1}
        // Result = {1, 1}  (min(3, 2) = 2)
        this.qst("""
                SELECT * FROM (VALUES (1), (1), (1)) AS T(x)
                INTERSECT ALL
                SELECT * FROM (VALUES (1), (1)) AS T(x);
                 x
                ---
                 1
                 1
                (2 rows)""");
    }

    @Test
    public void testIntersectAllEmpty() {
        // No common values: result is empty.
        this.qst("""
                SELECT * FROM (VALUES (1), (2)) AS T(x)
                INTERSECT ALL
                SELECT * FROM (VALUES (3), (4)) AS T(x);
                 x
                ---
                (0 rows)""");
    }

    @Test
    public void testIntersectAllNull() {
        // A = {NULL, NULL, 1}  B = {NULL, 1, 1}
        // Result = {NULL, 1}  (min(2,1)=1 for NULL, min(1,2)=1 for 1)
        this.qst("""
                SELECT * FROM (VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)), (1)) AS T(x)
                INTERSECT ALL
                SELECT * FROM (VALUES (CAST(NULL AS INT)), (1), (1)) AS T(x);
                 x
                ------
                 NULL
                 1
                (2 rows)""");
    }

    @Test
    public void testIntersectAllNullBothSides() {
        // A = {NULL, NULL}  B = {NULL, NULL, NULL}
        // Result = {NULL, NULL}  (min(2, 3) = 2)
        this.qst("""
                SELECT * FROM (VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT))) AS T(x)
                INTERSECT ALL
                SELECT * FROM (VALUES (CAST(NULL AS INT)), (CAST(NULL AS INT)), (CAST(NULL AS INT))) AS T(x);
                 x
                ------
                 NULL
                 NULL
                (2 rows)""");
    }

    @Test
    public void testIntersectAllMultiColumn() {
        // A = {(1,'a')x2, (2,NULL)x1, (NULL,'c')x1}
        // B = {(1,'a')x1, (2,NULL)x2, (NULL,'c')x1}
        // Result: each value appears min(count_A, count_B) times:
        //   (1,'a'): min(2,1) = 1
        //   (2,NULL): min(1,2) = 1
        //   (NULL,'c'): min(1,1) = 1
        this.qst("""
                SELECT * FROM (VALUES (1, 'a'), (1, 'a'), (2, CAST(NULL AS VARCHAR)), (CAST(NULL AS INT), 'c')) AS T(x, s)
                INTERSECT ALL
                SELECT * FROM (VALUES (1, 'a'), (2, CAST(NULL AS VARCHAR)), (2, CAST(NULL AS VARCHAR)), (CAST(NULL AS INT), 'c')) AS T(x, s);
                 x    | s
                ----------
                 1    | a
                 2    |NULL
                NULL  | c
                (3 rows)""");
    }

    @Test
    public void testIntersectAllThreeWay() {
        // A = {1, 1, 2, 3}  B = {1, 2, 2}  C = {1, 1, 1, 2}
        // Result: 1 → min(2,1,3)=1;  2 → min(1,2,1)=1;  3 → min(1,0,0)=0
        this.qst("""
                SELECT * FROM (VALUES (1), (1), (2), (3)) AS A(x)
                INTERSECT ALL
                SELECT * FROM (VALUES (1), (2), (2)) AS B(x)
                INTERSECT ALL
                SELECT * FROM (VALUES (1), (1), (1), (2)) AS C(x);
                 x
                ---
                 1
                 2
                (2 rows)""");
    }

    @Test
    public void testExceptBasic() {
        this.qst("""
                SELECT * FROM (VALUES (1), (2), (3)) AS T(x)
                EXCEPT
                SELECT * FROM (VALUES (2), (4)) AS T(x);
                 x
                ---
                 1
                 3
                (2 rows)""");
    }

    @Test
    public void testExceptRemovesAllDuplicates() {
        // EXCEPT works on distinct values: all copies of 2 are removed.
        this.qst("""
                SELECT * FROM (VALUES (1), (2), (2)) AS T(x)
                EXCEPT
                SELECT * FROM (VALUES (2)) AS T(x);
                 x
                ---
                 1
                (1 row)""");
    }

    @Test
    public void testExceptNull() {
        // NULL IS NOT DISTINCT FROM NULL: NULL on the right removes NULL from the left.
        this.qst("""
                SELECT * FROM (VALUES (CAST(NULL AS INT)), (1), (2)) AS T(x)
                EXCEPT
                SELECT * FROM (VALUES (CAST(NULL AS INT)), (3)) AS T(x);
                 x
                ---
                 1
                 2
                (2 rows)""");
    }

    @Test
    public void testExceptNullKept() {
        // NULL only on the left: kept in the result.
        this.qst("""
                SELECT * FROM (VALUES (CAST(NULL AS INT)), (1)) AS T(x)
                EXCEPT
                SELECT * FROM (VALUES (2), (3)) AS T(x);
                 x
                ------
                 NULL
                 1
                (2 rows)""");
    }

    @Test
    public void testExceptNullExcluded() {
        // NULL only on the right: does not affect non-null left values.
        this.qst("""
                SELECT * FROM (VALUES (2), (3)) AS T(x)
                EXCEPT
                SELECT * FROM (VALUES (CAST(NULL AS INT)), (3)) AS T(x);
                 x
                ---
                 2
                (1 row)""");
    }

    @Test
    public void testExceptEmpty() {
        // Right side contains all left values: result is empty.
        this.qst("""
                SELECT * FROM (VALUES (1), (2)) AS T(x)
                EXCEPT
                SELECT * FROM (VALUES (1), (2), (3)) AS T(x);
                 x
                ---
                (0 rows)""");
    }

    @Test
    public void testExceptMultiField() {
        // (1,10) is removed; (3,30) is kept because (3,99) does not match it.
        this.qst("""
                SELECT * FROM (VALUES (1, 10), (2, 20), (3, 30)) AS T(x, y)
                EXCEPT
                SELECT * FROM (VALUES (1, 10), (3, 99)) AS T(x, y);
                 x | y
                -------
                 2 | 20
                 3 | 30
                (2 rows)""");
    }

    /**
     * Runs one INTERSECT or EXCEPT test over a two-INT-column table.
     * Each column of each input row is either 10 (non-null) or NULL,
     * controlled by the four boolean flags. Rows match iff every column
     * pair is IS NOT DISTINCT FROM equal, which reduces to string equality
     * since we only use two distinct values.
     */
    private void checkSetOpNullTwoInts(
            String op, boolean nullInFirst, boolean nullInSecond,
            boolean nullOnLeft,  boolean nullOnRight) {
        String lx = (nullOnLeft  && nullInFirst)  ? "CAST(NULL AS INT)" : "10";
        String ly = (nullOnLeft  && nullInSecond) ? "CAST(NULL AS INT)" : "10";
        String rx = (nullOnRight && nullInFirst)  ? "CAST(NULL AS INT)" : "10";
        String ry = (nullOnRight && nullInSecond) ? "CAST(NULL AS INT)" : "10";

        boolean rowsMatch = lx.equals(rx) && ly.equals(ry);
        boolean hasResult = op.equals("INTERSECT") == rowsMatch;

        String resultX = lx.contains("NULL") ? "NULL" : "10";
        String resultY = ly.contains("NULL") ? "NULL" : "10";

        String sql = "SELECT * FROM (VALUES (" + lx + ", " + ly + ")) AS T(x, y)\n"
                + op + "\n"
                + "SELECT * FROM (VALUES (" + rx + ", " + ry + ")) AS T(x, y)";

        String expected = hasResult
                ? " x | y\n---\n " + resultX + " | " + resultY + "\n(1 row)"
                : " x | y\n---\n(0 rows)";

        this.qst(sql + ";\n" + expected);
    }

    /** EXCEPT: all 16 combinations of which column(s) hold NULL and on which side. */
    @Test
    public void testExceptNullTwoInts() {
        for (boolean nullInFirst : new boolean[]{true, false}) {
            for (boolean nullInSecond : new boolean[]{true, false}) {
                for (boolean nullOnLeft : new boolean[]{true, false}) {
                    for (boolean nullOnRight : new boolean[]{true, false}) {
                        checkSetOpNullTwoInts("EXCEPT", nullInFirst, nullInSecond, nullOnLeft, nullOnRight);
                    }
                }
            }
        }
    }

    /** INTERSECT: all 16 combinations of which column(s) hold NULL and on which side. */
    @Test
    public void testIntersectNullTwoInts() {
        for (boolean nullInFirst : new boolean[]{true, false}) {
            for (boolean nullInSecond : new boolean[]{true, false}) {
                for (boolean nullOnLeft : new boolean[]{true, false}) {
                    for (boolean nullOnRight : new boolean[]{true, false}) {
                        checkSetOpNullTwoInts("INTERSECT", nullInFirst, nullInSecond, nullOnLeft, nullOnRight);
                    }
                }
            }
        }
    }
}
