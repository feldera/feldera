package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Ignore;
import org.junit.Test;

public class SubQueryTests extends ScottBaseTests {
    @Test @Ignore("Until the issue is fixed")
    public void calciteIssue6985() {
        this.qs("""
                SELECT *
                from emp as e
                where exists (
                  select empno
                  from emp as ee
                  where e.empno = ee.empno or e.comm >= ee.sal
                );
                +-------+--------+-----------+------+------------+---------+---------+--------+
                | EMPNO | ENAME  | JOB       | MGR  | HIREDATE   | SAL     | COMM    | DEPTNO |
                +-------+--------+-----------+------+------------+---------+---------+--------+
                |  7369 | SMITH  | CLERK     | 7902 | 1980-12-17 |  800.00 |         |     20 |
                |  7499 | ALLEN  | SALESMAN  | 7698 | 1981-02-20 | 1600.00 |  300.00 |     30 |
                |  7521 | WARD   | SALESMAN  | 7698 | 1981-02-22 | 1250.00 |  500.00 |     30 |
                |  7566 | JONES  | MANAGER   | 7839 | 1981-02-04 | 2975.00 |         |     20 |
                |  7654 | MARTIN | SALESMAN  | 7698 | 1981-09-28 | 1250.00 | 1400.00 |     30 |
                |  7698 | BLAKE  | MANAGER   | 7839 | 1981-01-05 | 2850.00 |         |     30 |
                |  7782 | CLARK  | MANAGER   | 7839 | 1981-06-09 | 2450.00 |         |     10 |
                |  7788 | SCOTT  | ANALYST   | 7566 | 1987-04-19 | 3000.00 |         |     20 |
                |  7839 | KING   | PRESIDENT |      | 1981-11-17 | 5000.00 |         |     10 |
                |  7844 | TURNER | SALESMAN  | 7698 | 1981-09-08 | 1500.00 |    0.00 |     30 |
                |  7876 | ADAMS  | CLERK     | 7788 | 1987-05-23 | 1100.00 |         |     20 |
                |  7900 | JAMES  | CLERK     | 7698 | 1981-12-03 |  950.00 |         |     30 |
                |  7902 | FORD   | ANALYST   | 7566 | 1981-12-03 | 3000.00 |         |     20 |
                |  7934 | MILLER | CLERK     | 7782 | 1982-01-23 | 1300.00 |         |     10 |
                +-------+--------+-----------+------+------------+---------+---------+--------+
                (14 rows)
                
                SELECT
                  e1.COMM,
                  EXISTS (
                    SELECT 1
                    FROM EMP e2
                    WHERE e2.COMM IS NULL OR e2.COMM > e1.COMM * 10
                  ) AS exists_flag
                FROM EMP e1;
                 COMM   | EXISTS_FLAG
                --------+-------------
                   0.00 | true
                1400.00 | true
                 300.00 | true
                 500.00 | true
                        | true
                        | true
                        | true
                        | true
                        | true
                        | true
                        | true
                        | true
                        | true
                        | true
                (14 rows)
                
                select t.comm, t5.f1 is not null as exists_flag
                from (
                  select empno, ename, job, mgr, hiredate, sal, comm, deptno, comm * 10 as f8
                  from emp
                ) as t
                left join (
                  select t2.f8, true as f1
                  from (
                    select empno, ename, job, mgr, hiredate, sal, comm, deptno, cast(comm as decimal(17, 2)) as comm0
                    from emp
                  ) as t0
                  inner join (
                    select comm * 10 as f8
                    from emp
                    group by comm * 10
                  ) as t2
                  on t0.comm is null or t0.comm0 > t2.f8
                  group by t2.f8
                ) as t5
                on t.f8 is not distinct from t5.f8;
                 COMM   | EXISTS_FLAG
                --------+-------------
                   0.00 | true
                1400.00 | true
                 300.00 | true
                 500.00 | true
                        | true
                        | true
                        | true
                        | true
                        | true
                        | true
                        | true
                        | true
                        | true
                        | true
                (14 rows)""", TestOptimizations.Unoptimized);
    }

    @Test
    public void testOrderByLimit() {
        // Validated using MySQL
        this.qs("""
                SELECT comm AS comm
                FROM EMP where 30 = EMP.deptno
                ORDER BY comm limit 1;
                +------------+
                | comm       |
                +------------+
                |            |
                +------------+
                (1 rows)
                
                SELECT comm AS comm
                FROM EMP where 30 = EMP.deptno
                ORDER BY comm desc limit 1;
                +------------+
                | comm       |
                +------------+
                | 1400       |
                +------------+
                (1 rows)""");
    }

    @Test
    public void firstValueTests() {
        // All these produce calls to FIRST_VALUE from the expansion
        // Modified and tested on MySQL, since Calcite treats NULLs differently.
        this.qs("""
                SELECT dname FROM DEPT WHERE 2000 > (SELECT EMP.sal FROM EMP where
                DEPT.deptno = EMP.deptno ORDER BY year(hiredate), EMP.sal limit 1);
                +----------+
                | DNAME    |
                +----------+
                | RESEARCH |
                | SALES    |
                +----------+
                (2 rows)
                
                SELECT dname
                FROM DEPT
                WHERE 2000 > (SELECT EMP.sal
                              FROM EMP where DEPT.deptno = EMP.deptno and mgr > 8000
                              ORDER BY year(hiredate), EMP.sal limit 1);
                +-------+
                | DNAME |
                +-------+
                +-------+
                (0 rows)
                
                SELECT dname,
                     (SELECT EMP.comm
                      FROM EMP where DEPT.deptno = EMP.deptno
                      ORDER BY EMP.comm desc limit 1)
                FROM DEPT;
                +------------+--------+
                | DNAME      | EXPR$1 |
                +------------+--------+
                | ACCOUNTING |        |
                | OPERATIONS |        |
                | RESEARCH   |        |
                | SALES      | 1400   |
                +------------+--------+
                (4 rows)
                
                SELECT dname,
                    (SELECT EMP.sal
                     FROM EMP where DEPT.deptno = EMP.deptno
                     ORDER BY year(hiredate), EMP.sal limit 1)
                FROM DEPT;
                +------------+---------+
                | DNAME      | EXPR$1  |
                +------------+---------+
                | ACCOUNTING | 2450.00 |
                | OPERATIONS |         |
                | RESEARCH   |  800.00 |
                | SALES      |  950.00 |
                +------------+---------+
                (4 rows)
                
                SELECT dname,
                   (SELECT EMP.sal
                    FROM EMP where DEPT.deptno = EMP.deptno and mgr > 8000
                    ORDER BY year(hiredate), EMP.sal limit 1)
                FROM DEPT;
                +------------+--------+
                | DNAME      | EXPR$1 |
                +------------+--------+
                | ACCOUNTING |        |
                | OPERATIONS |        |
                | RESEARCH   |        |
                | SALES      |        |
                +------------+--------+
                (4 rows)""");
    }

    @Test @Ignore("Cannot be decorrelated")
    public void testFirst1() {
        this.qs("""
                select sal from EMP e
                where mod(cast(rand() as int), 2) = 3 OR 123 IN (
                    select cast(null as int)
                    from DEPT d
                    where d.deptno = e.deptno);
                 SAL
                -----
                (0 rows)
                
                select sal from EMP e
                where 123 NOT IN (
                    select cast(null as int)
                    from DEPT d
                    where e.deptno=d.deptno);
                 SAL
                -----
                (0 rows)
                
                select sal from EMP e
                where 10 NOT IN (
                    select deptno
                    from DEPT d
                    where e.deptno=d.deptno);
                 SAL
                ---------
                 1100.00
                 1250.00
                 1250.00
                 1500.00
                 1600.00
                 2850.00
                 2975.00
                 3000.00
                 3000.00
                  800.00
                  950.00
                (11 rows)
                
                select sal from EMP e
                where 10 NOT IN (
                    select case when true then deptno else null end
                    from DEPT d
                    where e.deptno=d.deptno);
                 SAL
                ---------
                 1100.00
                 1250.00
                 1250.00
                 1500.00
                 1600.00
                 2850.00
                 2975.00
                 3000.00
                 3000.00
                  800.00
                  950.00
                (11 rows)""");
    }
}
