package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Ignore;
import org.junit.Test;

// based on calcite/core/src/test/resources/sql/winagg.iq
public class WinAggTests extends ScottBaseTests {
    @Test
    public void testWindows0() {
        this.qs("""
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
                (14 rows)""", false);
    }

    @Test @Ignore("ROWS not yet implemented in WINDOW https://github.com/feldera/feldera/issues/457")
    public void testWindows() {
        this.qs("""
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
                |  7654 | 585.95 |
                |  7698 | 585.95 |
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
                (6 rows)""", false);
    }
}
