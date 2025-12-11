package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Test;

/** Tests from calcite select.iq */
public class SelectTests extends ScottBaseTests {
    @Test
    public void testExclude() {
        // issue 5216
        this.qs("""
                select * exclude(empno, ename, job, mgr) from emp order by hiredate limit 1;
                +------------+--------+------+--------+
                | HIREDATE   | SAL    | COMM | DEPTNO |
                +------------+--------+------+--------+
                | 1980-12-17 | 800.00 |      |     20 |
                +------------+--------+------+--------+
                (1 row)
                
                select * exclude(empno, ename, job, mgr, mgr) from emp order by hiredate limit 1;
                +------------+--------+------+--------+
                | HIREDATE   | SAL    | COMM | DEPTNO |
                +------------+--------+------+--------+
                | 1980-12-17 | 800.00 |      |     20 |
                +------------+--------+------+--------+
                (1 row)
                
                select e.*, d.* from emp e join dept d on e.deptno = d.deptno order by hiredate limit 1;
                +-------+-------+---------+------+------------+--------+------+--------+---------+---------+-------+
                | EMPNO | ENAME | JOB     | MGR  | HIREDATE   | SAL    | COMM | DEPTNO | DEPTNO0 | DNAME   | LOC   |
                +-------+-------+---------+------+------------+--------+------+--------+---------+---------+-------+
                |  7369 | SMITH| CLERK|     7902 | 1980-12-17 | 800.00 |      |     20 |      20 | RESEARCH| DALLAS|
                +-------+-------+---------+------+------------+--------+------+--------+---------+---------+-------+
                (1 row)
                
                select e.* exclude(e.empno, e.ename, e.job, e.mgr)
                from emp e join dept d on e.deptno = d.deptno order by hiredate limit 1;
                +------------+--------+------+--------+
                | HIREDATE   | SAL    | COMM | DEPTNO |
                +------------+--------+------+--------+
                | 1980-12-17 | 800.00 |      |     20 |
                +------------+--------+------+--------+
                (1 row)
                
                select e.* exclude(e.empno, e.ename, e.job, e.mgr), d.* exclude(d.dname)
                from emp e join dept d on e.deptno = d.deptno order by hiredate limit 1;
                +------------+---------+------+--------+---------+--------+
                | HIREDATE   | SAL     | COMM | DEPTNO | DEPTNO0 | LOC    |
                +------------+---------+------+--------+---------+--------+
                | 1980-12-17 | 800.00  |      |     20 |      20 | DALLAS|
                +------------+---------+------+--------+---------+--------+
                (1 row)""");
    }
}
