package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Ignore;
import org.junit.Test;

public class FoodmartPivotTests extends FoodmartBaseTests {
    @Test
    public void testPivot() {
        this.qs("""
                SELECT *
                FROM   (SELECT deptno, job, sal
                        FROM   emp)
                PIVOT  (SUM(sal) AS sum_sal, COUNT(*) AS "COUNT"
                        FOR (job) IN ('CLERK', 'MANAGER' mgr, 'ANALYST' AS "a"));
                +--------+-----------------+---------------+-------------+-----------+-----------+---------+
                | DEPTNO | 'CLERK'_SUM_SAL | 'CLERK'_COUNT | MGR_SUM_SAL | MGR_COUNT | a_SUM_SAL | a_COUNT |
                +--------+-----------------+---------------+-------------+-----------+-----------+---------+
                |     10 |         1300.00 |             1 |     2450.00 |         1 |           |       0 |
                |     20 |         1900.00 |             2 |     2975.00 |         1 |   6000.00 |       2 |
                |     30 |          950.00 |             1 |     2850.00 |         1 |           |       0 |
                +--------+-----------------+---------------+-------------+-----------+-----------+---------+
                (3 rows)

                SELECT *
                FROM (SELECT job, deptno FROM emp)
                PIVOT (COUNT(*) AS "COUNT" FOR deptno IN (10, 50, 20));
                +-----------+----------+----------+----------+
                | JOB       | 10_COUNT | 50_COUNT | 20_COUNT |
                +-----------+----------+----------+----------+
                | ANALYST|           0 |        0 |        2 |
                | CLERK|             1 |        0 |        2 |
                | MANAGER|           1 |        0 |        1 |
                | PRESIDENT|         1 |        0 |        0 |
                | SALESMAN|          0 |        0 |        0 |
                +-----------+----------+----------+----------+
                (5 rows)

                SELECT *
                FROM (SELECT job, deptno FROM emp)
                PIVOT (COUNT(*) AS "COUNT" FOR deptno IN (10, 50, 20)) AS e
                WHERE e.job <> 'MANAGER';
                +-----------+----------+----------+----------+
                | JOB       | 10_COUNT | 50_COUNT | 20_COUNT |
                +-----------+----------+----------+----------+
                | ANALYST|           0 |        0 |        2 |
                | CLERK|             1 |        0 |        2 |
                | PRESIDENT|         1 |        0 |        0 |
                | SALESMAN|          0 |        0 |        0 |
                +-----------+----------+----------+----------+
                (4 rows)

                SELECT job, SUM("10_COUNT") AS sum10, SUM("20_COUNT" + "50_COUNT") AS sum20
                FROM (SELECT job, deptno FROM emp)
                PIVOT (COUNT(*) AS "COUNT" FOR deptno IN (10, 50, 20)) AS e
                WHERE e.job <> 'MANAGER'
                GROUP BY job;
                +-----------+-------+-------+
                | JOB       | SUM10 | SUM20 |
                +-----------+-------+-------+
                | ANALYST|        0 |     2 |
                | CLERK|          1 |     2 |
                | PRESIDENT|      1 |     0 |
                | SALESMAN|       0 |     0 |
                +-----------+-------+-------+
                (4 rows)

                SELECT *
                FROM (SELECT mgr, deptno, job, sal FROM emp)
                PIVOT (SUM(sal) AS ss, COUNT(*) AS cc
                   FOR (job, deptno)
                   IN (('CLERK', 20) AS c20, ('MANAGER', 10) AS m10))
                -- ORDER BY 1 NULLS FIRST
                ;
                +------+---------+-------+---------+-------+
                | MGR  | C20_SS  | C20_C | M10_SS  | M10_C |
                +------+---------+-------+---------+-------+
                |      |         |     0 |         |     0 |
                | 7566 |         |     0 |         |     0 |
                | 7698 |         |     0 |         |     0 |
                | 7782 |         |     0 |         |     0 |
                | 7788 | 1100.00 |     1 |         |     0 |
                | 7839 |         |     0 | 2450.00 |     1 |
                | 7902 |  800.00 |     1 |         |     0 |
                +------+---------+-------+---------+-------+
                (7 rows)

                SELECT mgr,
                  SUM(sal) FILTER (WHERE job = 'CLERK' AND deptno = 20) AS c20_ss,
                  COUNT(*) FILTER (WHERE job = 'CLERK' AND deptno = 20) AS c20_c,
                  SUM(sal) FILTER (WHERE job = 'MANAGER' AND deptno = 10) AS m10_ss,
                  COUNT(*) FILTER (WHERE job = 'MANAGER' AND deptno = 10) AS m10_c
                FROM emp
                GROUP BY mgr
                -- ORDER BY 1 NULLS FIRST
                ;
                +------+---------+-------+---------+-------+
                | MGR  | C20_SS  | C20_C | M10_SS  | M10_C |
                +------+---------+-------+---------+-------+
                |      |         |     0 |         |     0 |
                | 7566 |         |     0 |         |     0 |
                | 7698 |         |     0 |         |     0 |
                | 7782 |         |     0 |         |     0 |
                | 7788 | 1100.00 |     1 |         |     0 |
                | 7839 |         |     0 | 2450.00 |     1 |
                | 7902 |  800.00 |     1 |         |     0 |
                +------+---------+-------+---------+-------+
                (7 rows)

                SELECT mgr,
                  SUM(CASE WHEN job = 'CLERK' AND deptno = 20 THEN sal END) c20_ss,
                  COUNT(CASE WHEN job = 'CLERK' AND deptno = 20 THEN 1 END) c20_c,
                  SUM(CASE WHEN job = 'MANAGER' AND deptno = 10 THEN sal END) m10_ss,
                  COUNT(CASE WHEN job = 'MANAGER' AND deptno = 10 THEN 1 END) m10_c
                FROM emp
                GROUP BY mgr
                -- ORDER BY 1 NULLS FIRST
                ;
                +------+---------+-------+---------+-------+
                | MGR  | C20_SS  | C20_C | M10_SS  | M10_C |
                +------+---------+-------+---------+-------+
                |      |         |     0 |         |     0 |
                | 7566 |         |     0 |         |     0 |
                | 7698 |         |     0 |         |     0 |
                | 7782 |         |     0 |         |     0 |
                | 7788 | 1100.00 |     1 |         |     0 |
                | 7839 |         |     0 | 2450.00 |     1 |
                | 7902 |  800.00 |     1 |         |     0 |
                +------+---------+-------+---------+-------+
                (7 rows)

                SELECT *
                FROM (SELECT deptno, mgr FROM   emp)
                PIVOT (COUNT(*) AS cc FOR mgr IN (7839, null, 7698))
                -- ORDER BY deptno
                ;
                +--------+--------+--------+--------+
                | DEPTNO | 7839_C | NULL_C | 7698_C |
                +--------+--------+--------+--------+
                |     10 |      1 |      0 |      0 |
                |     20 |      1 |      0 |      0 |
                |     30 |      1 |      0 |      5 |
                +--------+--------+--------+--------+
                (3 rows)

                SELECT *
                FROM   (SELECT job, deptno FROM emp)
                PIVOT  (COUNT(*) AS cc FOR (deptno,deptno) IN ((10,10), (30,20)));
                +-----------+---------+---------+
                | JOB       | 10_10_C | 30_20_C |
                +-----------+---------+---------+
                | ANALYST|          0 |       0 |
                | CLERK|            1 |       0 |
                | MANAGER|          1 |       0 |
                | PRESIDENT|        1 |       0 |
                | SALESMAN|         0 |       0 |
                +-----------+---------+---------+
                (5 rows)

                SELECT "20"
                FROM (SELECT deptno, sal FROM emp)
                PIVOT (SUM(sal) FOR (deptno) IN (10, 10, 20));
                +----------+
                | 20       |
                +----------+
                | 10875.00 |
                +----------+
                (1 row)

                SELECT *
                FROM (SELECT deptno, sal FROM emp)
                PIVOT (SUM(sal) FOR (deptno) IN (10, 10 as ten, 20));
                +---------+---------+----------+
                | 10      | TEN     | 20       |
                +---------+---------+----------+
                | 8750.00 | 8750.00 | 10875.00 |
                +---------+---------+----------+
                (1 row)

                SELECT a_cc, a_b_b_c
                FROM (SELECT sal, deptno FROM emp)
                PIVOT (SUM(sal) AS b_c, COUNT(*) AS cc FOR deptno IN (10 as a, 20 as a_b));
                +-----+----------+
                | A_C | A_B_B_C  |
                +-----+----------+
                |   3 | 10875.00 |
                +-----+----------+
                (1 row)

                SELECT *
                FROM (SELECT sal, CAST(empno as integer) as empno, deptno FROM emp)
                PIVOT (SUM(sal), SUM(empno) AS sum_empno FOR deptno IN (10, 20));
                +---------+--------------+----------+--------------+
                | 10      | 10_SUM_EMPNO | 20       | 20_SUM_EMPNO |
                +---------+--------------+----------+--------------+
                | 8750.00 |        23555 | 10875.00 |        38501 |
                +---------+--------------+----------+--------------+
                (1 row)

                SELECT 10_sum_empno
                FROM (SELECT sal, empno, deptno FROM emp)
                PIVOT (SUM(sal), COUNT(*), SUM(empno) AS sum_empno FOR deptno IN (10, 20));
                +--------------+
                | 10_SUM_EMPNO |
                +--------------+
                |        23555 |
                +--------------+
                (1 row)

                SELECT * FROM (SELECT sal, deptno, job, mgr FROM Emp)
                PIVOT (sum(sal + deptno + 1)
                   FOR job in ('CLERK' AS cc, 'ANALYST' AS a));
                +------+---------+---------+
                | MGR  | C       | A       |
                +------+---------+---------+
                | 7566 |         | 6042.00 |
                | 7698 |  981.00 |         |
                | 7782 | 1311.00 |         |
                | 7788 | 1121.00 |         |
                | 7839 |         |         |
                | 7902 |  821.00 |         |
                |      |         |         |
                +------+---------+---------+
                (7 rows)

                SELECT *
                FROM (
                 SELECT deptno, job, sal,
                   CASE WHEN ename < 'F' THEN 'F' ELSE 'M' END AS gender
                 FROM emp)
                PIVOT (sum(sal) AS ss, count(*) AS cc
                     FOR (job, deptno) IN (('CLERK', 10) AS C10,
                                           ('CLERK', 20) AS C20,
                                           ('ANALYST', 20) AS A20));
                +--------+---------+-------+---------+-------+---------+-------+
                | GENDER | C10_SS  | C10_C | C20_SS  | C20_C | A20_SS  | A20_C |
                +--------+---------+-------+---------+-------+---------+-------+
                | F|               |     0 | 1100.00 |     1 |         |     0 |
                | M|       1300.00 |     1 |  800.00 |     1 | 6000.00 |     2 |
                +--------+---------+-------+---------+-------+---------+-------+
                (2 rows)

                SELECT CASE WHEN ename < 'F' THEN 'F' ELSE 'M' END AS gender,
                    deptno, job, sal
                FROM emp
                WHERE (job, deptno) IN (('CLERK', 10), ('CLERK', 20), ('ANALYST', 20))
                -- ORDER BY gender, deptno, job
                ;
                +--------+--------+---------+---------+
                | GENDER | DEPTNO | JOB     | SAL     |
                +--------+--------+---------+---------+
                | F|           20 | CLERK|    1100.00 |
                | M|           10 | CLERK|    1300.00 |
                | M|           20 | ANALYST|  3000.00 |
                | M|           20 | ANALYST|  3000.00 |
                | M|           20 | CLERK|     800.00 |
                +--------+--------+---------+---------+
                (5 rows)""");
    }

    @Test @Ignore("UNPIVOT not yet implemented")
    public void unpivotTests() {
        this.qs("""
                SELECT *
                FROM (
                  SELECT *
                  FROM (
                     SELECT deptno, job, sal,
                       CASE WHEN ename < 'F' THEN 'F' ELSE 'M' END AS gender
                     FROM emp)
                  PIVOT (sum(sal) AS ss, count(*) AS cc
                         FOR (job, deptno)
                         IN (('CLERK', 10) AS C10,
                             ('CLERK', 20) AS C20,
                             ('ANALYST', 20) AS A20)))
                UNPIVOT (
                  (sum_sal, count_star)
                  FOR (job, deptno)
                  IN ((c10_ss, c10_cc) AS ('CLERK', 10),
                      (c20_ss, c20_cc) AS ('CLERK', 20),
                      (a20_ss, a20_cc) AS ('ANALYST', 20)));
                +--------+---------+--------+---------+------------+
                | GENDER | JOB     | DEPTNO | SUM_SAL | COUNT_STAR |
                +--------+---------+--------+---------+------------+
                | F      | ANALYST |     20 |         |          0 |
                | F      | CLERK   |     10 |         |          0 |
                | F      | CLERK   |     20 | 1100.00 |          1 |
                | M      | ANALYST |     20 | 6000.00 |          2 |
                | M      | CLERK   |     10 | 1300.00 |          1 |
                | M      | CLERK   |     20 |  800.00 |          1 |
                +--------+---------+--------+---------+------------+
                (6 rows)

                SELECT e.gender,
                    t.job,
                    t.deptno,
                    CASE
                      WHEN t.job = 'CLERK' AND t.deptno = 10 THEN c10_ss
                      WHEN t.job = 'CLERK' AND t.deptno = 20 THEN c20_ss
                      WHEN t.job = 'ANALYST' AND t.deptno = 20 THEN a20_ss
                    END AS sum_sal,
                    CASE
                      WHEN t.job = 'CLERK' AND t.deptno = 10 THEN c10_cc
                      WHEN t.job = 'CLERK' AND t.deptno = 20 THEN c20_cc
                      WHEN t.job = 'ANALYST' AND t.deptno = 20 THEN a20_cc
                    END AS count_star
                FROM (
                  SELECT *
                  FROM (
                    SELECT deptno, job, sal,
                        CASE WHEN ename < 'F' THEN 'F' ELSE 'M' END AS gender
                    FROM emp)
                  PIVOT (sum(sal) AS ss, count(*) AS cc
                     FOR (job, deptno) IN (('CLERK', 10) AS C10,
                                           ('CLERK', 20) AS C20,
                                           ('ANALYST', 20) AS A20))) AS e
                CROSS JOIN (VALUES ('CLERK', 10),
                                   ('CLERK', 20),
                                   ('ANALYST', 20)) AS t (job, deptno);
                +--------+---------+--------+---------+------------+
                | GENDER | JOB     | DEPTNO | SUM_SAL | COUNT_STAR |
                +--------+---------+--------+---------+------------+
                | F      | ANALYST |     20 |         |          0 |
                | F      | CLERK   |     10 |         |          0 |
                | F      | CLERK   |     20 | 1100.00 |          1 |
                | M      | ANALYST |     20 | 6000.00 |          2 |
                | M      | CLERK   |     10 | 1300.00 |          1 |
                | M      | CLERK   |     20 |  800.00 |          1 |
                +--------+---------+--------+---------+------------+
                (6 rows)

                SELECT e.gender, t.*
                FROM (
                  SELECT *
                  FROM (
                    SELECT deptno, job, sal,
                        CASE WHEN ename < 'F' THEN 'F' ELSE 'M' END AS gender
                    FROM emp)
                  PIVOT (sum(sal) AS ss, count(*) AS cc
                     FOR (job, deptno) IN (('CLERK', 10) AS C10,
                                           ('CLERK', 20) AS C20,
                                           ('ANALYST', 20) AS A20))) AS e
                CROSS JOIN LATERAL (VALUES
                   ('CLERK', 10, e.c10_ss, e.c10_cc),
                   ('CLERK', 20, e.c20_ss, e.c20_cc),
                   ('ANALYST', 20, e.a20_ss, e.a20_cc)) AS t (job, deptno, sum_sal, count_star);
                +--------+---------+--------+---------+------------+
                | GENDER | JOB     | DEPTNO | SUM_SAL | COUNT_STAR |
                +--------+---------+--------+---------+------------+
                | F      | ANALYST |     20 |         |          0 |
                | F      | CLERK   |     10 |         |          0 |
                | F      | CLERK   |     20 | 1100.00 |          1 |
                | M      | ANALYST |     20 | 6000.00 |          2 |
                | M      | CLERK   |     10 | 1300.00 |          1 |
                | M      | CLERK   |     20 |  800.00 |          1 |
                +--------+---------+--------+---------+------------+
                (6 rows)

                SELECT *
                FROM (
                  SELECT *
                  FROM (
                     SELECT deptno, job, sal,
                       CASE WHEN ename < 'F' THEN 'F' ELSE 'M' END AS gender
                     FROM emp)
                  PIVOT (sum(sal) AS ss, count(*) AS cc
                         FOR (job, deptno)
                         IN (('CLERK', 10) AS C10,
                             ('CLERK', 20) AS C20,
                             ('ANALYST', 20) AS A20)))
                UNPIVOT INCLUDE NULLS (
                  (sum_sal)
                  FOR (job, deptno)
                  IN ((c10_ss) AS ('CLERK', 10),
                      (c20_ss) AS ('CLERK', 20),
                      (c20_ss) AS ('CLERK', 20),
                      (c10_ss) AS ('ANALYST', 20)));
                +--------+-------+-------+---------+-------+---------+--------+---------+
                | GENDER | C10_C | C20_C | A20_SS  | A20_C | JOB     | DEPTNO | SUM_SAL |
                +--------+-------+-------+---------+-------+---------+--------+---------+
                | F      |     0 |     1 |         |     0 | ANALYST |     20 |         |
                | F      |     0 |     1 |         |     0 | CLERK   |     10 |         |
                | F      |     0 |     1 |         |     0 | CLERK   |     20 | 1100.00 |
                | F      |     0 |     1 |         |     0 | CLERK   |     20 | 1100.00 |
                | M      |     1 |     1 | 6000.00 |     2 | ANALYST |     20 | 1300.00 |
                | M      |     1 |     1 | 6000.00 |     2 | CLERK   |     10 | 1300.00 |
                | M      |     1 |     1 | 6000.00 |     2 | CLERK   |     20 |  800.00 |
                | M      |     1 |     1 | 6000.00 |     2 | CLERK   |     20 |  800.00 |
                +--------+-------+-------+---------+-------+---------+--------+---------+
                (8 rows)

                SELECT *
                FROM (
                  SELECT *
                  FROM (
                     SELECT deptno, job, sal,
                       CASE WHEN ename < 'F' THEN 'F' ELSE 'M' END AS gender
                     FROM emp)
                  PIVOT (sum(sal) AS ss, count(*) AS cc
                         FOR (job, deptno)
                         IN (('CLERK', 10) AS C10,
                             ('CLERK', 20) AS C20,
                             ('ANALYST', 20) AS A20)))
                UNPIVOT (
                  (sum_sal)
                  FOR (job, deptno)
                  IN ((c10_ss) AS ('CLERK', 10),
                      (c20_ss) AS ('CLERK', 20),
                      (c20_ss) AS ('CLERK', 20),
                      (c10_ss) AS ('ANALYST', 20)));
                +--------+-------+-------+---------+-------+---------+--------+---------+
                | GENDER | C10_C | C20_C | A20_SS  | A20_C | JOB     | DEPTNO | SUM_SAL |
                +--------+-------+-------+---------+-------+---------+--------+---------+
                | F      |     0 |     1 |         |     0 | CLERK   |     20 | 1100.00 |
                | F      |     0 |     1 |         |     0 | CLERK   |     20 | 1100.00 |
                | M      |     1 |     1 | 6000.00 |     2 | ANALYST |     20 | 1300.00 |
                | M      |     1 |     1 | 6000.00 |     2 | CLERK   |     10 | 1300.00 |
                | M      |     1 |     1 | 6000.00 |     2 | CLERK   |     20 |  800.00 |
                | M      |     1 |     1 | 6000.00 |     2 | CLERK   |     20 |  800.00 |
                +--------+-------+-------+---------+-------+---------+--------+---------+
                (6 rows)

                SELECT *
                FROM emp
                UNPIVOT (remuneration
                  FOR remuneration_type IN (comm, sal));
                +-------+--------+-----------+------+------------+--------+-------------------+--------------+
                | EMPNO | ENAME  | JOB       | MGR  | HIREDATE   | DEPTNO | REMUNERATION_TYPE | REMUNERATION |
                +-------+--------+-----------+------+------------+--------+-------------------+--------------+
                |  7369 | SMITH  | CLERK     | 7902 | 1980-12-17 |     20 | SAL               |       800.00 |
                |  7499 | ALLEN  | SALESMAN  | 7698 | 1981-02-20 |     30 | COMM              |       300.00 |
                |  7499 | ALLEN  | SALESMAN  | 7698 | 1981-02-20 |     30 | SAL               |      1600.00 |
                |  7521 | WARD   | SALESMAN  | 7698 | 1981-02-22 |     30 | COMM              |       500.00 |
                |  7521 | WARD   | SALESMAN  | 7698 | 1981-02-22 |     30 | SAL               |      1250.00 |
                |  7566 | JONES  | MANAGER   | 7839 | 1981-02-04 |     20 | SAL               |      2975.00 |
                |  7654 | MARTIN | SALESMAN  | 7698 | 1981-09-28 |     30 | COMM              |      1400.00 |
                |  7654 | MARTIN | SALESMAN  | 7698 | 1981-09-28 |     30 | SAL               |      1250.00 |
                |  7698 | BLAKE  | MANAGER   | 7839 | 1981-01-05 |     30 | SAL               |      2850.00 |
                |  7782 | CLARK  | MANAGER   | 7839 | 1981-06-09 |     10 | SAL               |      2450.00 |
                |  7788 | SCOTT  | ANALYST   | 7566 | 1987-04-19 |     20 | SAL               |      3000.00 |
                |  7839 | KING   | PRESIDENT |      | 1981-11-17 |     10 | SAL               |      5000.00 |
                |  7844 | TURNER | SALESMAN  | 7698 | 1981-09-08 |     30 | COMM              |         0.00 |
                |  7844 | TURNER | SALESMAN  | 7698 | 1981-09-08 |     30 | SAL               |      1500.00 |
                |  7876 | ADAMS  | CLERK     | 7788 | 1987-05-23 |     20 | SAL               |      1100.00 |
                |  7900 | JAMES  | CLERK     | 7698 | 1981-12-03 |     30 | SAL               |       950.00 |
                |  7902 | FORD   | ANALYST   | 7566 | 1981-12-03 |     20 | SAL               |      3000.00 |
                |  7934 | MILLER | CLERK     | 7782 | 1982-01-23 |     10 | SAL               |      1300.00 |
                +-------+--------+-----------+------+------------+--------+-------------------+--------------+
                (18 rows)

                SELECT *
                FROM emp
                UNPIVOT INCLUDE NULLS (remuneration
                  FOR remuneration_type IN (comm, sal));
                +-------+--------+-----------+------+------------+--------+-------------------+--------------+
                | EMPNO | ENAME  | JOB       | MGR  | HIREDATE   | DEPTNO | REMUNERATION_TYPE | REMUNERATION |
                +-------+--------+-----------+------+------------+--------+-------------------+--------------+
                |  7369 | SMITH  | CLERK     | 7902 | 1980-12-17 |     20 | COMM              |              |
                |  7369 | SMITH  | CLERK     | 7902 | 1980-12-17 |     20 | SAL               |       800.00 |
                |  7499 | ALLEN  | SALESMAN  | 7698 | 1981-02-20 |     30 | COMM              |       300.00 |
                |  7499 | ALLEN  | SALESMAN  | 7698 | 1981-02-20 |     30 | SAL               |      1600.00 |
                |  7521 | WARD   | SALESMAN  | 7698 | 1981-02-22 |     30 | COMM              |       500.00 |
                |  7521 | WARD   | SALESMAN  | 7698 | 1981-02-22 |     30 | SAL               |      1250.00 |
                |  7566 | JONES  | MANAGER   | 7839 | 1981-02-04 |     20 | COMM              |              |
                |  7566 | JONES  | MANAGER   | 7839 | 1981-02-04 |     20 | SAL               |      2975.00 |
                |  7654 | MARTIN | SALESMAN  | 7698 | 1981-09-28 |     30 | COMM              |      1400.00 |
                |  7654 | MARTIN | SALESMAN  | 7698 | 1981-09-28 |     30 | SAL               |      1250.00 |
                |  7698 | BLAKE  | MANAGER   | 7839 | 1981-01-05 |     30 | COMM              |              |
                |  7698 | BLAKE  | MANAGER   | 7839 | 1981-01-05 |     30 | SAL               |      2850.00 |
                |  7782 | CLARK  | MANAGER   | 7839 | 1981-06-09 |     10 | COMM              |              |
                |  7782 | CLARK  | MANAGER   | 7839 | 1981-06-09 |     10 | SAL               |      2450.00 |
                |  7788 | SCOTT  | ANALYST   | 7566 | 1987-04-19 |     20 | COMM              |              |
                |  7788 | SCOTT  | ANALYST   | 7566 | 1987-04-19 |     20 | SAL               |      3000.00 |
                |  7839 | KING   | PRESIDENT |      | 1981-11-17 |     10 | COMM              |              |
                |  7839 | KING   | PRESIDENT |      | 1981-11-17 |     10 | SAL               |      5000.00 |
                |  7844 | TURNER | SALESMAN  | 7698 | 1981-09-08 |     30 | COMM              |         0.00 |
                |  7844 | TURNER | SALESMAN  | 7698 | 1981-09-08 |     30 | SAL               |      1500.00 |
                |  7876 | ADAMS  | CLERK     | 7788 | 1987-05-23 |     20 | COMM              |              |
                |  7876 | ADAMS  | CLERK     | 7788 | 1987-05-23 |     20 | SAL               |      1100.00 |
                |  7900 | JAMES  | CLERK     | 7698 | 1981-12-03 |     30 | COMM              |              |
                |  7900 | JAMES  | CLERK     | 7698 | 1981-12-03 |     30 | SAL               |       950.00 |
                |  7902 | FORD   | ANALYST   | 7566 | 1981-12-03 |     20 | COMM              |              |
                |  7902 | FORD   | ANALYST   | 7566 | 1981-12-03 |     20 | SAL               |      3000.00 |
                |  7934 | MILLER | CLERK     | 7782 | 1982-01-23 |     10 | COMM              |              |
                |  7934 | MILLER | CLERK     | 7782 | 1982-01-23 |     10 | SAL               |      1300.00 |
                +-------+--------+-----------+------+------------+--------+-------------------+--------------+
                (28 rows)

                SELECT *
                FROM emp
                UNPIVOT INCLUDE NULLS (remuneration
                  FOR remuneration_type IN (comm, sal))
                WHERE deptno = 20 AND remuneration > 500;
                +-------+-------+---------+------+------------+--------+-------------------+--------------+
                | EMPNO | ENAME | JOB     | MGR  | HIREDATE   | DEPTNO | REMUNERATION_TYPE | REMUNERATION |
                +-------+-------+---------+------+------------+--------+-------------------+--------------+
                |  7369 | SMITH | CLERK   | 7902 | 1980-12-17 |     20 | SAL               |       800.00 |
                |  7566 | JONES | MANAGER | 7839 | 1981-02-04 |     20 | SAL               |      2975.00 |
                |  7788 | SCOTT | ANALYST | 7566 | 1987-04-19 |     20 | SAL               |      3000.00 |
                |  7876 | ADAMS | CLERK   | 7788 | 1987-05-23 |     20 | SAL               |      1100.00 |
                |  7902 | FORD  | ANALYST | 7566 | 1981-12-03 |     20 | SAL               |      3000.00 |
                +-------+-------+---------+------+------------+--------+-------------------+--------------+
                (5 rows)

                SELECT deptno,
                  SUM(remuneration) AS r,
                  SUM(remuneration) FILTER (WHERE job = 'CLERK') AS cr
                FROM emp
                UNPIVOT INCLUDE NULLS (remuneration
                  FOR remuneration_type IN (comm, sal))
                GROUP BY deptno
                HAVING COUNT(*) > 6
                -- ORDER BY deptno;
                +--------+----------+---------+
                | DEPTNO | R        | CR      |
                +--------+----------+---------+
                |     20 | 10875.00 | 1900.00 |
                |     30 | 11600.00 |  950.00 |
                +--------+----------+---------+
                (2 rows)

                SELECT *
                FROM emp
                UNPIVOT (remuneration
                  FOR sal IN (comm AS 'commission',
                                sal as 'salary'));
                +-------+--------+-----------+------+------------+--------+------------+--------------+
                | EMPNO | ENAME  | JOB       | MGR  | HIREDATE   | DEPTNO | SAL        | REMUNERATION |
                +-------+--------+-----------+------+------------+--------+------------+--------------+
                |  7369 | SMITH  | CLERK     | 7902 | 1980-12-17 |     20 | salary     |       800.00 |
                |  7499 | ALLEN  | SALESMAN  | 7698 | 1981-02-20 |     30 | commission |       300.00 |
                |  7499 | ALLEN  | SALESMAN  | 7698 | 1981-02-20 |     30 | salary     |      1600.00 |
                |  7521 | WARD   | SALESMAN  | 7698 | 1981-02-22 |     30 | commission |       500.00 |
                |  7521 | WARD   | SALESMAN  | 7698 | 1981-02-22 |     30 | salary     |      1250.00 |
                |  7566 | JONES  | MANAGER   | 7839 | 1981-02-04 |     20 | salary     |      2975.00 |
                |  7654 | MARTIN | SALESMAN  | 7698 | 1981-09-28 |     30 | commission |      1400.00 |
                |  7654 | MARTIN | SALESMAN  | 7698 | 1981-09-28 |     30 | salary     |      1250.00 |
                |  7698 | BLAKE  | MANAGER   | 7839 | 1981-01-05 |     30 | salary     |      2850.00 |
                |  7782 | CLARK  | MANAGER   | 7839 | 1981-06-09 |     10 | salary     |      2450.00 |
                |  7788 | SCOTT  | ANALYST   | 7566 | 1987-04-19 |     20 | salary     |      3000.00 |
                |  7839 | KING   | PRESIDENT |      | 1981-11-17 |     10 | salary     |      5000.00 |
                |  7844 | TURNER | SALESMAN  | 7698 | 1981-09-08 |     30 | commission |         0.00 |
                |  7844 | TURNER | SALESMAN  | 7698 | 1981-09-08 |     30 | salary     |      1500.00 |
                |  7876 | ADAMS  | CLERK     | 7788 | 1987-05-23 |     20 | salary     |      1100.00 |
                |  7900 | JAMES  | CLERK     | 7698 | 1981-12-03 |     30 | salary     |       950.00 |
                |  7902 | FORD   | ANALYST   | 7566 | 1981-12-03 |     20 | salary     |      3000.00 |
                |  7934 | MILLER | CLERK     | 7782 | 1982-01-23 |     10 | salary     |      1300.00 |
                +-------+--------+-----------+------+------------+--------+------------+--------------+
                (18 rows)

                SELECT *
                FROM (
                  SELECT *
                  FROM (VALUES (0, 1, 2, 3, 4),
                               (10, 11, 12, 13, 14))
                          AS t (c0, c1, c2, c3, c4))
                UNPIVOT ((m0, m1, m2)
                    FOR (a0, a1)
                     IN ((c1, c2, c3) as ('col1','col2'),
                         (c2, c3, c4)));
                +----+----------+----------+----+----+----+
                | C0 | A0       | A1       | M0 | M1 | M2 |
                +----+----------+----------+----+----+----+
                |  0 | col1     | col2     |  1 |  2 |  3 |
                |  0 | C2_C3_C4 | C2_C3_C4 |  2 |  3 |  4 |
                | 10 | col1     | col2     | 11 | 12 | 13 |
                | 10 | C2_C3_C4 | C2_C3_C4 | 12 | 13 | 14 |
                +----+----------+----------+----+----+----+
                (4 rows)""");
    }
}
