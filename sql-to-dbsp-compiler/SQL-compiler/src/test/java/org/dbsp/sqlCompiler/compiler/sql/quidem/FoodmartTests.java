package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Ignore;
import org.junit.Test;

public class FoodmartTests extends FoodmartBaseTests {
    @Test@Ignore("Calcite desugars qualify into WINDOW aggregate with ROWS; not yet implemented")
    public void testQualify() {
        this.q("""
                SELECT empno, ename, deptno, job, mgr
                FROM emp
                QUALIFY ROW_NUMBER() over (partition by ename order by deptno) = 1;
                 empno | ename | deptno | job | mgr
                ------------------------------------""");
    }

    @Test
    public void testSelect() {
        this.q("""
                SELECT * FROM DEPT;
                DEPT NO | DNAME | LOC
                ---------------------
                10 | ACCOUNTING| NEW YORK
                20 | RESEARCH| DALLAS
                30 | SALES| CHICAGO
                40 | OPERATIONS| BOSTON""");
    }

    @Test
    public void testScalar() {
        this.qs("""
                select deptno, (select min(empno) from emp where deptno = dept.deptno) as x from dept;
                +--------+------+
                | DEPTNO | X    |
                +--------+------+
                |     10 | 7782 |
                |     20 | 7369 |
                |     30 | 7499 |
                |     40 |      |
                +--------+------+
                (4 rows)

                select deptno, (select count(*) from emp where deptno = dept.deptno) as x from dept;
                +--------+---+
                | DEPTNO | X |
                +--------+---+
                |     10 | 3 |
                |     20 | 5 |
                |     30 | 6 |
                |     40 | 0 |
                +--------+---+
                (4 rows)

                select deptno, (select count(*) from emp where deptno = dept.deptno group by deptno) as x from dept;
                +--------+---+
                | DEPTNO | X |
                +--------+---+
                |     10 | 3 |
                |     20 | 5 |
                |     30 | 6 |
                |     40 |   |
                +--------+---+
                (4 rows)

                select deptno, (select sum(cast(empno as int)) from emp where deptno = dept.deptno group by deptno) as x from dept;
                +--------+-------+
                | DEPTNO | X     |
                +--------+-------+
                |     10 | 23555 |
                |     20 | 38501 |
                |     30 | 46116 |
                |     40 |       |
                +--------+-------+
                (4 rows)

                select deptno, (select count(*) from emp where 1 = 0) as x from dept;
                +--------+---+
                | DEPTNO | X |
                +--------+---+
                |     10 | 0 |
                |     20 | 0 |
                |     30 | 0 |
                |     40 | 0 |
                +--------+---+
                (4 rows)

                select deptno, (select count(*) from emp where 1 = 0 group by ()) as x from dept;
                +--------+---+
                | DEPTNO | X |
                +--------+---+
                |     10 | 0 |
                |     20 | 0 |
                |     30 | 0 |
                |     40 | 0 |
                +--------+---+
                (4 rows)

                select deptno, (select sum(empno) from emp where 1 = 0) as x from dept;
                +--------+---+
                | DEPTNO | X |
                +--------+---+
                |     10 |   |
                |     20 |   |
                |     30 |   |
                |     40 |   |
                +--------+---+
                (4 rows)

                select deptno, (select empno from emp where 1 = 0) as x from dept;
                +--------+---+
                | DEPTNO | X |
                +--------+---+
                |     10 |   |
                |     20 |   |
                |     30 |   |
                |     40 |   |
                +--------+---+
                (4 rows)

                select deptno, (select empno from emp where emp.deptno = dept.deptno and job = 'PRESIDENT') as x from dept;
                +--------+------+
                | DEPTNO | X    |
                +--------+------+
                |     10 | 7839 |
                |     20 |      |
                |     30 |      |
                |     40 |      |
                +--------+------+
                (4 rows)

                select deptno, (select sum(empno) from emp where 1 = 0 group by ()) as x from dept;
                +--------+---+
                | DEPTNO | X |
                +--------+---+
                |     10 |   |
                |     20 |   |
                |     30 |   |
                |     40 |   |
                +--------+---+
                (4 rows)
                """);
    }

    @Test @Ignore("Cannot be decorrelated (generates LATERAL)")
    public void limitTests() {
        this.qs("""
                select deptno, (select sum(empno) from emp where deptno = dept.deptno limit 1) as x from dept;
                +--------+----------------------+
                | DEPTNO |          X           |
                +--------+----------------------+
                | 10     | 23555                |
                | 20     | 38501                |
                | 30     | 46116                |
                | 40     | null                 |
                +--------+----------------------+
                (4 rows)

                select deptno, (select sum(empno) from emp where deptno = dept.deptno limit 0) as x from dept;
                +--------+----------------------+
                | DEPTNO |          X           |
                +--------+----------------------+
                | 10     | 23555                |
                | 20     | 38501                |
                | 30     | 46116                |
                | 40     | null                 |
                +--------+----------------------+
                (4 rows)

                select deptno, (select deptno from emp where deptno = dept.deptno limit 1) as x from dept;
                +--------+------+
                | DEPTNO |  X   |
                +--------+------+
                | 10     | 10   |
                | 20     | 20   |
                | 30     | 30   |
                | 40     | null |
                +--------+------+
                (4 rows)

                select deptno, (select deptno from emp where deptno = dept.deptno limit 0) as x from dept;
                +--------+---+
                | DEPTNO | X |
                +--------+---+
                |     10 |   |
                |     20 |   |
                |     30 |   |
                |     40 |   |
                +--------+---+
                (4 rows)

                select deptno, (select empno from emp where deptno = dept.deptno order by empno limit 1) as x from dept;
                +--------+--------+
                | DEPTNO |   X    |
                +--------+--------+
                | 10     | 7369   |
                | 20     | 7369   |
                | 30     | 7369   |
                | 40     | 7369   |
                +--------+--------+
                (4 rows)

                select deptno, (select empno from emp order by empno limit 1) as x from dept;
                +--------+------+
                | DEPTNO | X    |
                +--------+------+
                |     10 | 7369 |
                |     20 | 7369 |
                |     30 | 7369 |
                |     40 | 7369 |
                +--------+------+
                (4 rows)""");
    }
}
