package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Ignore;
import org.junit.Test;

// based on calcite/core/src/test/resources/sql/winagg.iq
public class WinAggTests extends ScottBaseTests {
    @Test
    public void testWindows0() {
        this.qst("""
                select empno, deptno,
                 count(*) over (order by deptno) c1,
                 count(*) over (order by deptno range unbounded preceding) c2,
                 count(*) over (order by deptno range between unbounded preceding and current row) c3,
                 count(*) over (order by deptno range between unbounded preceding and unbounded following) c6
                from emp;
                +-------+--------+----+----+----+----+
                | EMPNO | DEPTNO | C1 | C2 | C3 | C6 |
                +-------+--------+----+----+----+----+
                |  7900 |     30 | 14 | 14 | 14 | 14 |
                |  7902 |     20 |  8 |  8 |  8 | 14 |
                |  7934 |     10 |  3 |  3 |  3 | 14 |
                |  7369 |     20 |  8 |  8 |  8 | 14 |
                |  7499 |     30 | 14 | 14 | 14 | 14 |
                |  7521 |     30 | 14 | 14 | 14 | 14 |
                |  7566 |     20 |  8 |  8 |  8 | 14 |
                |  7654 |     30 | 14 | 14 | 14 | 14 |
                |  7698 |     30 | 14 | 14 | 14 | 14 |
                |  7782 |     10 |  3 |  3 |  3 | 14 |
                |  7788 |     20 |  8 |  8 |  8 | 14 |
                |  7839 |     10 |  3 |  3 |  3 | 14 |
                |  7844 |     30 | 14 | 14 | 14 | 14 |
                |  7876 |     20 |  8 |  8 |  8 | 14 |
                +-------+--------+----+----+----+----+
                (14 rows)""");
    }

    @Test @Ignore("We don't support windows without ORDER BY spec")
    public void testWindowIllegal() {
        this.qst("""
                -- [CALCITE-6538] OVER (ROWS CURRENT ROW) should return a window with one row, not all rows
                -- (RANGE CURRENT ROW returns all rows, and is already correct.)
                select ename,
                  sal,
                  sum(sal) over (rows current row) as row_sum_sal,
                  sum(sal) over (range current row) as range_sum_sal
                from emp
                where job = 'MANAGER';
                +-------+---------+-------------+---------------+
                | ENAME | SAL     | ROW_SUM_SAL | RANGE_SUM_SAL |
                +-------+---------+-------------+---------------+
                | BLAKE | 2850.00 |     2850.00 |       8275.00 |
                | CLARK | 2450.00 |     2450.00 |       8275.00 |
                | JONES | 2975.00 |     2975.00 |       8275.00 |
                +-------+---------+-------------+---------------+
                (3 rows)
                
                select count(*) over (rows between 3 preceding and 2 preceding) as c3p2p,
                  sum(two) over (rows between 3 preceding and 2 preceding) as s3p2p,
                  sum(two) over (rows 1 preceding) as s1p,
                  sum(two) over (rows between 1 preceding and 1 following) as s1p1f,
                  sum(two) over (rows between current row and 2 following) as s2f
                from (select *, 2 as two from emp)
                order by 1;
                +-------+-------+-----+-------+-----+
                | C3P2P | S3P2P | S1P | S1P1F | S2F |
                +-------+-------+-----+-------+-----+
                |     0 |       |   2 |     4 |   6 |
                |     0 |       |   4 |     6 |   6 |
                |     1 |     2 |   4 |     6 |   6 |
                |     2 |     4 |   4 |     6 |   6 |
                |     2 |     4 |   4 |     6 |   6 |
                |     2 |     4 |   4 |     6 |   6 |
                |     2 |     4 |   4 |     6 |   6 |
                |     2 |     4 |   4 |     6 |   6 |
                |     2 |     4 |   4 |     6 |   6 |
                |     2 |     4 |   4 |     6 |   6 |
                |     2 |     4 |   4 |     6 |   6 |
                |     2 |     4 |   4 |     6 |   6 |
                |     2 |     4 |   4 |     6 |   4 |
                |     2 |     4 |   4 |     4 |   2 |
                +-------+-------+-----+-------+-----+
                (14 rows)
                
                -- Various combinations of ROWS, RANGE, UNBOUNDED PRECEDING,
                -- UNBOUNDED FOLLOWING, and CURRENT ROW.
                --
                -- 'OVER (ROWS CURRENT ROW)' includes one row;
                --
                -- 'OVER (ROWS UNBOUNDED PRECEDING)' includes the previous rows;
                --
                -- 'OVER ()' includes all rows and is equivalent to
                -- 'OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)' and
                -- 'OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)' and
                -- 'OVER (RANGE UNBOUNDED PRECEDING)'.
                -- Checked on Postgres.
                select count(*) over (rows current row) as "rows",
                  count(*) over (rows unbounded preceding) as "rowsUp",
                  count(*) over (range unbounded preceding) as "rangeUp",
                  count(*) over () as "empty",
                  count(*) over (rows between unbounded preceding and unbounded following) as "rowsUpUf",
                  count(*) over (range between unbounded preceding and unbounded following) as "rangeUpUf",
                  count(*) over (range unbounded preceding) as "rangeUp"
                from emp
                order by 3;
                +------+--------+---------+-------+----------+-----------+---------+
                | rows | rowsUp | rangeUp | empty | rowsUpUf | rangeUpUf | rangeUp |
                +------+--------+---------+-------+----------+-----------+---------+
                |    1 |      1 |      14 |    14 |       14 |        14 |      14 |
                |    1 |      2 |      14 |    14 |       14 |        14 |      14 |
                |    1 |      3 |      14 |    14 |       14 |        14 |      14 |
                |    1 |      4 |      14 |    14 |       14 |        14 |      14 |
                |    1 |      5 |      14 |    14 |       14 |        14 |      14 |
                |    1 |      6 |      14 |    14 |       14 |        14 |      14 |
                |    1 |      7 |      14 |    14 |       14 |        14 |      14 |
                |    1 |      8 |      14 |    14 |       14 |        14 |      14 |
                |    1 |      9 |      14 |    14 |       14 |        14 |      14 |
                |    1 |     10 |      14 |    14 |       14 |        14 |      14 |
                |    1 |     11 |      14 |    14 |       14 |        14 |      14 |
                |    1 |     12 |      14 |    14 |       14 |        14 |      14 |
                |    1 |     13 |      14 |    14 |       14 |        14 |      14 |
                |    1 |     14 |      14 |    14 |       14 |        14 |      14 |
                +------+--------+---------+-------+----------+-----------+---------+
                (14 rows)
                
                -- Various combinations of ROWS, RANGE, UNBOUNDED PRECEDING,
                -- UNBOUNDED FOLLOWING, and CURRENT ROW, with PARTITION BY.
                -- The equivalence sets are the same the previous query (without PARTITION BY).
                -- Checked on Postgres.
                select deptno,
                  count(*) over (partition by deptno rows current row) as "rows",
                  count(*) over (partition by deptno rows unbounded preceding) as "rowsUp",
                  count(*) over (partition by deptno range unbounded preceding) as "rangeUp",
                  count(*) over (partition by deptno) as "empty",
                  count(*) over (partition by deptno rows between unbounded preceding and unbounded following) as "rowsUpUf",
                  count(*) over (partition by deptno range between unbounded preceding and unbounded following) as "rangeUpUf",
                  count(*) over (partition by deptno range unbounded preceding) as "rangeUp"
                from emp
                order by 1, 4;
                +--------+------+--------+---------+-------+----------+-----------+---------+
                | DEPTNO | rows | rowsUp | rangeUp | empty | rowsUpUf | rangeUpUf | rangeUp |
                +--------+------+--------+---------+-------+----------+-----------+---------+
                |     10 |    1 |      1 |       3 |     3 |        3 |         3 |       3 |
                |     10 |    1 |      2 |       3 |     3 |        3 |         3 |       3 |
                |     10 |    1 |      3 |       3 |     3 |        3 |         3 |       3 |
                |     20 |    1 |      1 |       5 |     5 |        5 |         5 |       5 |
                |     20 |    1 |      2 |       5 |     5 |        5 |         5 |       5 |
                |     20 |    1 |      3 |       5 |     5 |        5 |         5 |       5 |
                |     20 |    1 |      4 |       5 |     5 |        5 |         5 |       5 |
                |     20 |    1 |      5 |       5 |     5 |        5 |         5 |       5 |
                |     30 |    1 |      1 |       6 |     6 |        6 |         6 |       6 |
                |     30 |    1 |      2 |       6 |     6 |        6 |         6 |       6 |
                |     30 |    1 |      3 |       6 |     6 |        6 |         6 |       6 |
                |     30 |    1 |      4 |       6 |     6 |        6 |         6 |       6 |
                |     30 |    1 |      5 |       6 |     6 |        6 |         6 |       6 |
                |     30 |    1 |      6 |       6 |     6 |        6 |         6 |       6 |
                +--------+------+--------+---------+-------+----------+-----------+---------+
                (14 rows)""");
    }

    @Test
    public void testWindowRows() {
        this.qst("""
                -- As previous, with PARTITION BY and ORDER BY.
                --
                -- 'OVER (... ROWS CURRENT ROW)' (rows) includes one row;
                --
                -- 'OVER (...)' (empty),
                -- 'OVER (... RANGE UNBOUNDED PRECEDING)' (rangeUp) and
                -- 'OVER (... RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)' (rangeUpC)
                -- are equivalent to each other,
                --
                -- 'OVER (... ROWS UNBOUNDED PRECEDING)' (rowsUp) and
                -- 'OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)' (rowsUpC)
                -- are equivalent to each other and include N rows,
                -- and are similar to empty/rangeUp/rangeUpC except when there are ties (WARD, SCOTT);
                --
                -- 'OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)' (rowsUpUf) and
                -- 'OVER (... RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)' (rangeUpUf)
                -- are equivalent and return all rows.
                --
                -- Checked on Postgres.
                select deptno, sal, ename,
                  count(*) over (partition by deptno order by sal rows current row) as "rows",
                  count(*) over (partition by deptno order by sal) as "empty",
                  count(*) over (partition by deptno order by sal range unbounded preceding) as "rangeUp",
                  count(*) over (partition by deptno order by sal range between unbounded preceding and current row) as "rangeUpC",
                  count(*) over (partition by deptno order by sal rows unbounded preceding) as "rowsUp",
                  count(*) over (partition by deptno order by sal rows between unbounded preceding and current row) as "rowsUpC",
                  count(*) over (partition by deptno order by sal range between unbounded preceding and unbounded following) as "rangeUpUf",
                  count(*) over (partition by deptno order by sal rows between unbounded preceding and unbounded following) as "rowsUpUf"
                from emp
                order by 1, 2;
                +--------+---------+--------+------+-------+---------+----------+--------+---------+-----------+----------+
                | DEPTNO | SAL     | ENAME  | rows | empty | rangeUp | rangeUpC | rowsUp | rowsUpC | rangeUpUf | rowsUpUf |
                +--------+---------+--------+------+-------+---------+----------+--------+---------+-----------+----------+
                |     10 | 1300.00 | MILLER |    1 |     1 |       1 |        1 |      1 |       1 |         3 |        3 |
                |     10 | 2450.00 | CLARK  |    1 |     2 |       2 |        2 |      2 |       2 |         3 |        3 |
                |     10 | 5000.00 | KING   |    1 |     3 |       3 |        3 |      3 |       3 |         3 |        3 |
                |     20 |  800.00 | SMITH  |    1 |     1 |       1 |        1 |      1 |       1 |         5 |        5 |
                |     20 | 1100.00 | ADAMS  |    1 |     2 |       2 |        2 |      2 |       2 |         5 |        5 |
                |     20 | 2975.00 | JONES  |    1 |     3 |       3 |        3 |      3 |       3 |         5 |        5 |
                |     20 | 3000.00 | SCOTT  |    1 |     5 |       5 |        5 |      4 |       4 |         5 |        5 |
                |     20 | 3000.00 | FORD   |    1 |     5 |       5 |        5 |      5 |       5 |         5 |        5 |
                |     30 |  950.00 | JAMES  |    1 |     1 |       1 |        1 |      1 |       1 |         6 |        6 |
                |     30 | 1250.00 | WARD   |    1 |     3 |       3 |        3 |      2 |       2 |         6 |        6 |
                |     30 | 1250.00 | MARTIN |    1 |     3 |       3 |        3 |      3 |       3 |         6 |        6 |
                |     30 | 1500.00 | TURNER |    1 |     4 |       4 |        4 |      4 |       4 |         6 |        6 |
                |     30 | 1600.00 | ALLEN  |    1 |     5 |       5 |        5 |      5 |       5 |         6 |        6 |
                |     30 | 2850.00 | BLAKE  |    1 |     6 |       6 |        6 |      6 |       6 |         6 |        6 |
                +--------+---------+--------+------+-------+---------+----------+--------+---------+-----------+----------+
                (14 rows)
                
                -- Check default brackets. Note that:
                -- c2 and c3 are equivalent to c1;
                -- c5 is equivalent to c4;
                -- c7 is equivalent to c6.
                select empno, deptno,
                 count(*) over (order by deptno) c1,
                 count(*) over (order by deptno range unbounded preceding) c2,
                 count(*) over (order by deptno range between unbounded preceding and current row) c3,
                 count(*) over (order by deptno rows unbounded preceding) c4,
                 count(*) over (order by deptno rows between unbounded preceding and current row) c5,
                 count(*) over (order by deptno range between unbounded preceding and unbounded following) c6,
                 count(*) over (order by deptno rows between unbounded preceding and unbounded following) c7
                from emp;
                +-------+--------+----+----+----+----+----+----+----+
                | EMPNO | DEPTNO | C1 | C2 | C3 | C4 | C5 | C6 | C7 |
                +-------+--------+----+----+----+----+----+----+----+
                |  7900 |     30 | 14 | 14 | 14 | 14 | 14 | 14 | 14 |
                |  7902 |     20 |  8 |  8 |  8 |  8 |  8 | 14 | 14 |
                |  7934 |     10 |  3 |  3 |  3 |  3 |  3 | 14 | 14 |
                |  7369 |     20 |  8 |  8 |  8 |  4 |  4 | 14 | 14 |
                |  7499 |     30 | 14 | 14 | 14 |  9 |  9 | 14 | 14 |
                |  7521 |     30 | 14 | 14 | 14 | 10 | 10 | 14 | 14 |
                |  7566 |     20 |  8 |  8 |  8 |  5 |  5 | 14 | 14 |
                |  7654 |     30 | 14 | 14 | 14 | 11 | 11 | 14 | 14 |
                |  7698 |     30 | 14 | 14 | 14 | 12 | 12 | 14 | 14 |
                |  7782 |     10 |  3 |  3 |  3 |  1 |  1 | 14 | 14 |
                |  7788 |     20 |  8 |  8 |  8 |  6 |  6 | 14 | 14 |
                |  7839 |     10 |  3 |  3 |  3 |  2 |  2 | 14 | 14 |
                |  7844 |     30 | 14 | 14 | 14 | 13 | 13 | 14 | 14 |
                |  7876 |     20 |  8 |  8 |  8 |  7 |  7 | 14 | 14 |
                +-------+--------+----+----+----+----+----+----+----+
                (14 rows)

                select empno,
                  count(comm) over (order by empno rows unbounded preceding) as c
                from emp
                where deptno = 30
                order by 1;
                +-------+---+
                | EMPNO | c |
                +-------+---+
                |  7499 | 1 |
                |  7521 | 2 |
                |  7654 | 3 |
                |  7698 | 3 |
                |  7844 | 4 |
                |  7900 | 4 |
                +-------+---+
                (6 rows)

                -- STDDEV applied to nullable column
                select empno,
                  stddev(CAST(comm AS DECIMAL(10, 2))) over (order by empno rows unbounded preceding) as stdev
                from emp
                where deptno = 30
                order by 1;
                +-------+--------+
                | EMPNO | STDEV  |
                +-------+--------+
                |  7499 |        |
                |  7521 | 141.42 |
                |  7654 | 585.94 |
                |  7698 | 585.94 |
                |  7844 | 602.77 |
                |  7900 | 602.77 |
                +-------+--------+
                (6 rows)

                -- [CALCITE-5931] Allow integers like 1.00 in window frame
                select empno,
                  stddev(CAST(comm AS DECIMAL(10, 2))) over (order by empno rows 2 preceding) as stdev_2int,
                  stddev(CAST(comm AS DECIMAL(10, 2))) over (order by empno rows 2.00 preceding) as stdev_2double
                from emp
                where deptno = 30
                order by 1;
                +-------+------------+---------------+
                | EMPNO | STDEV_2INT | STDEV_2DOUBLE |
                +-------+------------+---------------+
                |  7499 |            |               |
                |  7521 | 141.42     | 141.42        |
                |  7654 | 585.94     | 585.94        |
                |  7698 | 636.39     | 636.39        |
                |  7844 | 989.94     | 989.94        |
                |  7900 |            |               |
                +-------+------------+---------------+
                (6 rows)""");
    }
}
