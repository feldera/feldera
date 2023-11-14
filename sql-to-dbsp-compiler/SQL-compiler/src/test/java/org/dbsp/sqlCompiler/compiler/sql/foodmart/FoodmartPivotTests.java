package org.dbsp.sqlCompiler.compiler.sql.foodmart;

import org.junit.Ignore;
import org.junit.Test;

public class FoodmartPivotTests extends FoodmartBaseTests {
    @Test
    public void testPivot() {
        this.qs("SELECT *\n" +
                "FROM   (SELECT deptno, job, sal\n" +
                "        FROM   emp)\n" +
                "PIVOT  (SUM(sal) AS sum_sal, COUNT(*) AS \"COUNT\"\n" +
                "        FOR (job) IN ('CLERK', 'MANAGER' mgr, 'ANALYST' AS \"a\"));\n" +
                "+--------+-----------------+---------------+-------------+-----------+-----------+---------+\n" +
                "| DEPTNO | 'CLERK'_SUM_SAL | 'CLERK'_COUNT | MGR_SUM_SAL | MGR_COUNT | a_SUM_SAL | a_COUNT |\n" +
                "+--------+-----------------+---------------+-------------+-----------+-----------+---------+\n" +
                "|     10 |         1300.00 |             1 |     2450.00 |         1 |           |       0 |\n" +
                "|     20 |         1900.00 |             2 |     2975.00 |         1 |   6000.00 |       2 |\n" +
                "|     30 |          950.00 |             1 |     2850.00 |         1 |           |       0 |\n" +
                "+--------+-----------------+---------------+-------------+-----------+-----------+---------+\n" +
                "(3 rows)\n" +
                "\n" +
                "SELECT *\n" +
                "FROM (SELECT job, deptno FROM emp)\n" +
                "PIVOT (COUNT(*) AS \"COUNT\" FOR deptno IN (10, 50, 20));\n" +
                "+-----------+----------+----------+----------+\n" +
                "| JOB       | 10_COUNT | 50_COUNT | 20_COUNT |\n" +
                "+-----------+----------+----------+----------+\n" +
                "| ANALYST|        0 |        0 |        2 |\n" +
                "| CLERK|        1 |        0 |        2 |\n" +
                "| MANAGER|        1 |        0 |        1 |\n" +
                "| PRESIDENT|        1 |        0 |        0 |\n" +
                "| SALESMAN|        0 |        0 |        0 |\n" +
                "+-----------+----------+----------+----------+\n" +
                "(5 rows)\n" +
                "\n" +
                "SELECT *\n" +
                "FROM (SELECT job, deptno FROM emp)\n" +
                "PIVOT (COUNT(*) AS \"COUNT\" FOR deptno IN (10, 50, 20)) AS e\n" +
                "WHERE e.job <> 'MANAGER';\n" +
                "+-----------+----------+----------+----------+\n" +
                "| JOB       | 10_COUNT | 50_COUNT | 20_COUNT |\n" +
                "+-----------+----------+----------+----------+\n" +
                "| ANALYST|        0 |        0 |        2 |\n" +
                "| CLERK|        1 |        0 |        2 |\n" +
                "| PRESIDENT|        1 |        0 |        0 |\n" +
                "| SALESMAN|        0 |        0 |        0 |\n" +
                "+-----------+----------+----------+----------+\n" +
                "(4 rows)\n" +
                "\n" +
                "SELECT job, SUM(\"10_COUNT\") AS sum10, SUM(\"20_COUNT\" + \"50_COUNT\") AS sum20\n" +
                "FROM (SELECT job, deptno FROM emp)\n" +
                "PIVOT (COUNT(*) AS \"COUNT\" FOR deptno IN (10, 50, 20)) AS e\n" +
                "WHERE e.job <> 'MANAGER'\n" +
                "GROUP BY job;\n" +
                "+-----------+-------+-------+\n" +
                "| JOB       | SUM10 | SUM20 |\n" +
                "+-----------+-------+-------+\n" +
                "| ANALYST|     0 |     2 |\n" +
                "| CLERK|     1 |     2 |\n" +
                "| PRESIDENT|     1 |     0 |\n" +
                "| SALESMAN|     0 |     0 |\n" +
                "+-----------+-------+-------+\n" +
                "(4 rows)\n" +
                "\n" +
                "SELECT *\n" +
                "FROM (SELECT mgr, deptno, job, sal FROM emp)\n" +
                "PIVOT (SUM(sal) AS ss, COUNT(*) AS cc\n" +
                "   FOR (job, deptno)\n" +
                "   IN (('CLERK', 20) AS c20, ('MANAGER', 10) AS m10))\n" +
                "-- ORDER BY 1 NULLS FIRST" +
                ";\n" +
                "+------+---------+-------+---------+-------+\n" +
                "| MGR  | C20_SS  | C20_C | M10_SS  | M10_C |\n" +
                "+------+---------+-------+---------+-------+\n" +
                "|      |         |     0 |         |     0 |\n" +
                "| 7566 |         |     0 |         |     0 |\n" +
                "| 7698 |         |     0 |         |     0 |\n" +
                "| 7782 |         |     0 |         |     0 |\n" +
                "| 7788 | 1100.00 |     1 |         |     0 |\n" +
                "| 7839 |         |     0 | 2450.00 |     1 |\n" +
                "| 7902 |  800.00 |     1 |         |     0 |\n" +
                "+------+---------+-------+---------+-------+\n" +
                "(7 rows)\n" +
                "\n" +
                "SELECT mgr,\n" +
                "  SUM(sal) FILTER (WHERE job = 'CLERK' AND deptno = 20) AS c20_ss,\n" +
                "  COUNT(*) FILTER (WHERE job = 'CLERK' AND deptno = 20) AS c20_c,\n" +
                "  SUM(sal) FILTER (WHERE job = 'MANAGER' AND deptno = 10) AS m10_ss,\n" +
                "  COUNT(*) FILTER (WHERE job = 'MANAGER' AND deptno = 10) AS m10_c\n" +
                "FROM emp\n" +
                "GROUP BY mgr\n" +
                "-- ORDER BY 1 NULLS FIRST" +
                ";\n" +
                "+------+---------+-------+---------+-------+\n" +
                "| MGR  | C20_SS  | C20_C | M10_SS  | M10_C |\n" +
                "+------+---------+-------+---------+-------+\n" +
                "|      |         |     0 |         |     0 |\n" +
                "| 7566 |         |     0 |         |     0 |\n" +
                "| 7698 |         |     0 |         |     0 |\n" +
                "| 7782 |         |     0 |         |     0 |\n" +
                "| 7788 | 1100.00 |     1 |         |     0 |\n" +
                "| 7839 |         |     0 | 2450.00 |     1 |\n" +
                "| 7902 |  800.00 |     1 |         |     0 |\n" +
                "+------+---------+-------+---------+-------+\n" +
                "(7 rows)\n" +
                "\n" +
                "SELECT mgr,\n" +
                "  SUM(CASE WHEN job = 'CLERK' AND deptno = 20 THEN sal END) c20_ss,\n" +
                "  COUNT(CASE WHEN job = 'CLERK' AND deptno = 20 THEN 1 END) c20_c,\n" +
                "  SUM(CASE WHEN job = 'MANAGER' AND deptno = 10 THEN sal END) m10_ss,\n" +
                "  COUNT(CASE WHEN job = 'MANAGER' AND deptno = 10 THEN 1 END) m10_c\n" +
                "FROM emp\n" +
                "GROUP BY mgr\n" +
                "-- ORDER BY 1 NULLS FIRST" +
                ";\n" +
                "+------+---------+-------+---------+-------+\n" +
                "| MGR  | C20_SS  | C20_C | M10_SS  | M10_C |\n" +
                "+------+---------+-------+---------+-------+\n" +
                "|      |         |     0 |         |     0 |\n" +
                "| 7566 |         |     0 |         |     0 |\n" +
                "| 7698 |         |     0 |         |     0 |\n" +
                "| 7782 |         |     0 |         |     0 |\n" +
                "| 7788 | 1100.00 |     1 |         |     0 |\n" +
                "| 7839 |         |     0 | 2450.00 |     1 |\n" +
                "| 7902 |  800.00 |     1 |         |     0 |\n" +
                "+------+---------+-------+---------+-------+\n" +
                "(7 rows)\n" +
                "\n" +
                "SELECT *\n" +
                "FROM (SELECT deptno, mgr FROM   emp)\n" +
                "PIVOT (COUNT(*) AS cc FOR mgr IN (7839, null, 7698))\n" +
                "-- ORDER BY deptno" +
                ";\n" +
                "+--------+--------+--------+--------+\n" +
                "| DEPTNO | 7839_C | NULL_C | 7698_C |\n" +
                "+--------+--------+--------+--------+\n" +
                "|     10 |      1 |      0 |      0 |\n" +
                "|     20 |      1 |      0 |      0 |\n" +
                "|     30 |      1 |      0 |      5 |\n" +
                "+--------+--------+--------+--------+\n" +
                "(3 rows)\n" +
                "\n" +
                "SELECT *\n" +
                "FROM   (SELECT job, deptno FROM emp)\n" +
                "PIVOT  (COUNT(*) AS cc FOR (deptno,deptno) IN ((10,10), (30,20)));\n" +
                "+-----------+---------+---------+\n" +
                "| JOB       | 10_10_C | 30_20_C |\n" +
                "+-----------+---------+---------+\n" +
                "| ANALYST|       0 |       0 |\n" +
                "| CLERK|       1 |       0 |\n" +
                "| MANAGER|       1 |       0 |\n" +
                "| PRESIDENT|       1 |       0 |\n" +
                "| SALESMAN|       0 |       0 |\n" +
                "+-----------+---------+---------+\n" +
                "(5 rows)\n" +
                "\n" +
                "SELECT \"20\"\n" +
                "FROM (SELECT deptno, sal FROM emp)\n" +
                "PIVOT (SUM(sal) FOR (deptno) IN (10, 10, 20));\n" +
                "+----------+\n" +
                "| 20       |\n" +
                "+----------+\n" +
                "| 10875.00 |\n" +
                "+----------+\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT *\n" +
                "FROM (SELECT deptno, sal FROM emp)\n" +
                "PIVOT (SUM(sal) FOR (deptno) IN (10, 10 as ten, 20));\n" +
                "+---------+---------+----------+\n" +
                "| 10      | TEN     | 20       |\n" +
                "+---------+---------+----------+\n" +
                "| 8750.00 | 8750.00 | 10875.00 |\n" +
                "+---------+---------+----------+\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT a_cc, a_b_b_c\n" +
                "FROM (SELECT sal, deptno FROM emp)\n" +
                "PIVOT (SUM(sal) AS b_c, COUNT(*) AS cc FOR deptno IN (10 as a, 20 as a_b));\n" +
                "+-----+----------+\n" +
                "| A_C | A_B_B_C  |\n" +
                "+-----+----------+\n" +
                "|   3 | 10875.00 |\n" +
                "+-----+----------+\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT *\n" +
                "FROM (SELECT sal, CAST(empno as integer) as empno, deptno FROM emp)\n" +
                "PIVOT (SUM(sal), SUM(empno) AS sum_empno FOR deptno IN (10, 20));\n" +
                "+---------+--------------+----------+--------------+\n" +
                "| 10      | 10_SUM_EMPNO | 20       | 20_SUM_EMPNO |\n" +
                "+---------+--------------+----------+--------------+\n" +
                "| 8750.00 |        23555 | 10875.00 |        38501 |\n" +
                "+---------+--------------+----------+--------------+\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT \"10_SUM_EMPNO\"\n" +
                "FROM (SELECT sal, empno, deptno FROM emp)\n" +
                "PIVOT (SUM(sal), COUNT(*), SUM(empno) AS sum_empno FOR deptno IN (10, 20));\n" +
                "+--------------+\n" +
                "| 10_SUM_EMPNO |\n" +
                "+--------------+\n" +
                "|        23555 |\n" +
                "+--------------+\n" +
                "(1 row)\n" +
                "\n" +
                "SELECT * FROM (SELECT sal, deptno, job, mgr FROM Emp)\n" +
                "PIVOT (sum(sal + deptno + 1)\n" +
                "   FOR job in ('CLERK' AS cc, 'ANALYST' AS a));\n" +
                "+------+---------+---------+\n" +
                "| MGR  | C       | A       |\n" +
                "+------+---------+---------+\n" +
                "| 7566 |         | 6042.00 |\n" +
                "| 7698 |  981.00 |         |\n" +
                "| 7782 | 1311.00 |         |\n" +
                "| 7788 | 1121.00 |         |\n" +
                "| 7839 |         |         |\n" +
                "| 7902 |  821.00 |         |\n" +
                "|      |         |         |\n" +
                "+------+---------+---------+\n" +
                "(7 rows)\n" +
                "\n" +
                "SELECT *\n" +
                "FROM (\n" +
                " SELECT deptno, job, sal,\n" +
                "   CASE WHEN ename < 'F' THEN 'F' ELSE 'M' END AS gender\n" +
                " FROM emp)\n" +
                "PIVOT (sum(sal) AS ss, count(*) AS cc\n" +
                "     FOR (job, deptno) IN (('CLERK', 10) AS C10,\n" +
                "                           ('CLERK', 20) AS C20,\n" +
                "                           ('ANALYST', 20) AS A20));\n" +
                "+--------+---------+-------+---------+-------+---------+-------+\n" +
                "| GENDER | C10_SS  | C10_C | C20_SS  | C20_C | A20_SS  | A20_C |\n" +
                "+--------+---------+-------+---------+-------+---------+-------+\n" +
                "| F|         |     0 | 1100.00 |     1 |         |     0 |\n" +
                "| M| 1300.00 |     1 |  800.00 |     1 | 6000.00 |     2 |\n" +
                "+--------+---------+-------+---------+-------+---------+-------+\n" +
                "(2 rows)\n" +
                "\n" +
                "SELECT CASE WHEN ename < 'F' THEN 'F' ELSE 'M' END AS gender,\n" +
                "    deptno, job, sal\n" +
                "FROM emp\n" +
                "WHERE (job, deptno) IN (('CLERK', 10), ('CLERK', 20), ('ANALYST', 20))\n" +
                "-- ORDER BY gender, deptno, job" +
                ";\n" +
                "+--------+--------+---------+---------+\n" +
                "| GENDER | DEPTNO | JOB     | SAL     |\n" +
                "+--------+--------+---------+---------+\n" +
                "| F|     20 | CLERK| 1100.00 |\n" +
                "| M|     10 | CLERK| 1300.00 |\n" +
                "| M|     20 | ANALYST| 3000.00 |\n" +
                "| M|     20 | ANALYST| 3000.00 |\n" +
                "| M|     20 | CLERK|  800.00 |\n" +
                "+--------+--------+---------+---------+\n" +
                "(5 rows)");
    }

    @Test @Ignore("UNPIVOT not yet implemented")
    public void unpivotTests() {
        this.qs("SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM (\n" +
                "     SELECT deptno, job, sal,\n" +
                "       CASE WHEN ename < 'F' THEN 'F' ELSE 'M' END AS gender\n" +
                "     FROM emp)\n" +
                "  PIVOT (sum(sal) AS ss, count(*) AS cc\n" +
                "         FOR (job, deptno)\n" +
                "         IN (('CLERK', 10) AS C10,\n" +
                "             ('CLERK', 20) AS C20,\n" +
                "             ('ANALYST', 20) AS A20)))\n" +
                "UNPIVOT (\n" +
                "  (sum_sal, count_star)\n" +
                "  FOR (job, deptno)\n" +
                "  IN ((c10_ss, c10_cc) AS ('CLERK', 10),\n" +
                "      (c20_ss, c20_cc) AS ('CLERK', 20),\n" +
                "      (a20_ss, a20_cc) AS ('ANALYST', 20)));\n" +
                "+--------+---------+--------+---------+------------+\n" +
                "| GENDER | JOB     | DEPTNO | SUM_SAL | COUNT_STAR |\n" +
                "+--------+---------+--------+---------+------------+\n" +
                "| F      | ANALYST |     20 |         |          0 |\n" +
                "| F      | CLERK   |     10 |         |          0 |\n" +
                "| F      | CLERK   |     20 | 1100.00 |          1 |\n" +
                "| M      | ANALYST |     20 | 6000.00 |          2 |\n" +
                "| M      | CLERK   |     10 | 1300.00 |          1 |\n" +
                "| M      | CLERK   |     20 |  800.00 |          1 |\n" +
                "+--------+---------+--------+---------+------------+\n" +
                "(6 rows)\n" +
                "\n" +
                "SELECT e.gender,\n" +
                "    t.job,\n" +
                "    t.deptno,\n" +
                "    CASE\n" +
                "      WHEN t.job = 'CLERK' AND t.deptno = 10 THEN c10_ss\n" +
                "      WHEN t.job = 'CLERK' AND t.deptno = 20 THEN c20_ss\n" +
                "      WHEN t.job = 'ANALYST' AND t.deptno = 20 THEN a20_ss\n" +
                "    END AS sum_sal,\n" +
                "    CASE\n" +
                "      WHEN t.job = 'CLERK' AND t.deptno = 10 THEN c10_cc\n" +
                "      WHEN t.job = 'CLERK' AND t.deptno = 20 THEN c20_cc\n" +
                "      WHEN t.job = 'ANALYST' AND t.deptno = 20 THEN a20_cc\n" +
                "    END AS count_star\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM (\n" +
                "    SELECT deptno, job, sal,\n" +
                "        CASE WHEN ename < 'F' THEN 'F' ELSE 'M' END AS gender\n" +
                "    FROM emp)\n" +
                "  PIVOT (sum(sal) AS ss, count(*) AS cc\n" +
                "     FOR (job, deptno) IN (('CLERK', 10) AS C10,\n" +
                "                           ('CLERK', 20) AS C20,\n" +
                "                           ('ANALYST', 20) AS A20))) AS e\n" +
                "CROSS JOIN (VALUES ('CLERK', 10),\n" +
                "                   ('CLERK', 20),\n" +
                "                   ('ANALYST', 20)) AS t (job, deptno);\n" +
                "+--------+---------+--------+---------+------------+\n" +
                "| GENDER | JOB     | DEPTNO | SUM_SAL | COUNT_STAR |\n" +
                "+--------+---------+--------+---------+------------+\n" +
                "| F      | ANALYST |     20 |         |          0 |\n" +
                "| F      | CLERK   |     10 |         |          0 |\n" +
                "| F      | CLERK   |     20 | 1100.00 |          1 |\n" +
                "| M      | ANALYST |     20 | 6000.00 |          2 |\n" +
                "| M      | CLERK   |     10 | 1300.00 |          1 |\n" +
                "| M      | CLERK   |     20 |  800.00 |          1 |\n" +
                "+--------+---------+--------+---------+------------+\n" +
                "(6 rows)\n" +
                "\n" +
                "SELECT e.gender, t.*\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM (\n" +
                "    SELECT deptno, job, sal,\n" +
                "        CASE WHEN ename < 'F' THEN 'F' ELSE 'M' END AS gender\n" +
                "    FROM emp)\n" +
                "  PIVOT (sum(sal) AS ss, count(*) AS cc\n" +
                "     FOR (job, deptno) IN (('CLERK', 10) AS C10,\n" +
                "                           ('CLERK', 20) AS C20,\n" +
                "                           ('ANALYST', 20) AS A20))) AS e\n" +
                "CROSS JOIN LATERAL (VALUES\n" +
                "   ('CLERK', 10, e.c10_ss, e.c10_cc),\n" +
                "   ('CLERK', 20, e.c20_ss, e.c20_cc),\n" +
                "   ('ANALYST', 20, e.a20_ss, e.a20_cc)) AS t (job, deptno, sum_sal, count_star);\n" +
                "+--------+---------+--------+---------+------------+\n" +
                "| GENDER | JOB     | DEPTNO | SUM_SAL | COUNT_STAR |\n" +
                "+--------+---------+--------+---------+------------+\n" +
                "| F      | ANALYST |     20 |         |          0 |\n" +
                "| F      | CLERK   |     10 |         |          0 |\n" +
                "| F      | CLERK   |     20 | 1100.00 |          1 |\n" +
                "| M      | ANALYST |     20 | 6000.00 |          2 |\n" +
                "| M      | CLERK   |     10 | 1300.00 |          1 |\n" +
                "| M      | CLERK   |     20 |  800.00 |          1 |\n" +
                "+--------+---------+--------+---------+------------+\n" +
                "(6 rows)\n" +
                "\n" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM (\n" +
                "     SELECT deptno, job, sal,\n" +
                "       CASE WHEN ename < 'F' THEN 'F' ELSE 'M' END AS gender\n" +
                "     FROM emp)\n" +
                "  PIVOT (sum(sal) AS ss, count(*) AS cc\n" +
                "         FOR (job, deptno)\n" +
                "         IN (('CLERK', 10) AS C10,\n" +
                "             ('CLERK', 20) AS C20,\n" +
                "             ('ANALYST', 20) AS A20)))\n" +
                "UNPIVOT INCLUDE NULLS (\n" +
                "  (sum_sal)\n" +
                "  FOR (job, deptno)\n" +
                "  IN ((c10_ss) AS ('CLERK', 10),\n" +
                "      (c20_ss) AS ('CLERK', 20),\n" +
                "      (c20_ss) AS ('CLERK', 20),\n" +
                "      (c10_ss) AS ('ANALYST', 20)));\n" +
                "+--------+-------+-------+---------+-------+---------+--------+---------+\n" +
                "| GENDER | C10_C | C20_C | A20_SS  | A20_C | JOB     | DEPTNO | SUM_SAL |\n" +
                "+--------+-------+-------+---------+-------+---------+--------+---------+\n" +
                "| F      |     0 |     1 |         |     0 | ANALYST |     20 |         |\n" +
                "| F      |     0 |     1 |         |     0 | CLERK   |     10 |         |\n" +
                "| F      |     0 |     1 |         |     0 | CLERK   |     20 | 1100.00 |\n" +
                "| F      |     0 |     1 |         |     0 | CLERK   |     20 | 1100.00 |\n" +
                "| M      |     1 |     1 | 6000.00 |     2 | ANALYST |     20 | 1300.00 |\n" +
                "| M      |     1 |     1 | 6000.00 |     2 | CLERK   |     10 | 1300.00 |\n" +
                "| M      |     1 |     1 | 6000.00 |     2 | CLERK   |     20 |  800.00 |\n" +
                "| M      |     1 |     1 | 6000.00 |     2 | CLERK   |     20 |  800.00 |\n" +
                "+--------+-------+-------+---------+-------+---------+--------+---------+\n" +
                "(8 rows)\n" +
                "\n" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM (\n" +
                "     SELECT deptno, job, sal,\n" +
                "       CASE WHEN ename < 'F' THEN 'F' ELSE 'M' END AS gender\n" +
                "     FROM emp)\n" +
                "  PIVOT (sum(sal) AS ss, count(*) AS cc\n" +
                "         FOR (job, deptno)\n" +
                "         IN (('CLERK', 10) AS C10,\n" +
                "             ('CLERK', 20) AS C20,\n" +
                "             ('ANALYST', 20) AS A20)))\n" +
                "UNPIVOT (\n" +
                "  (sum_sal)\n" +
                "  FOR (job, deptno)\n" +
                "  IN ((c10_ss) AS ('CLERK', 10),\n" +
                "      (c20_ss) AS ('CLERK', 20),\n" +
                "      (c20_ss) AS ('CLERK', 20),\n" +
                "      (c10_ss) AS ('ANALYST', 20)));\n" +
                "+--------+-------+-------+---------+-------+---------+--------+---------+\n" +
                "| GENDER | C10_C | C20_C | A20_SS  | A20_C | JOB     | DEPTNO | SUM_SAL |\n" +
                "+--------+-------+-------+---------+-------+---------+--------+---------+\n" +
                "| F      |     0 |     1 |         |     0 | CLERK   |     20 | 1100.00 |\n" +
                "| F      |     0 |     1 |         |     0 | CLERK   |     20 | 1100.00 |\n" +
                "| M      |     1 |     1 | 6000.00 |     2 | ANALYST |     20 | 1300.00 |\n" +
                "| M      |     1 |     1 | 6000.00 |     2 | CLERK   |     10 | 1300.00 |\n" +
                "| M      |     1 |     1 | 6000.00 |     2 | CLERK   |     20 |  800.00 |\n" +
                "| M      |     1 |     1 | 6000.00 |     2 | CLERK   |     20 |  800.00 |\n" +
                "+--------+-------+-------+---------+-------+---------+--------+---------+\n" +
                "(6 rows)\n" +
                "\n" +
                "SELECT *\n" +
                "FROM emp\n" +
                "UNPIVOT (remuneration\n" +
                "  FOR remuneration_type IN (comm, sal));\n" +
                "+-------+--------+-----------+------+------------+--------+-------------------+--------------+\n" +
                "| EMPNO | ENAME  | JOB       | MGR  | HIREDATE   | DEPTNO | REMUNERATION_TYPE | REMUNERATION |\n" +
                "+-------+--------+-----------+------+------------+--------+-------------------+--------------+\n" +
                "|  7369 | SMITH  | CLERK     | 7902 | 1980-12-17 |     20 | SAL               |       800.00 |\n" +
                "|  7499 | ALLEN  | SALESMAN  | 7698 | 1981-02-20 |     30 | COMM              |       300.00 |\n" +
                "|  7499 | ALLEN  | SALESMAN  | 7698 | 1981-02-20 |     30 | SAL               |      1600.00 |\n" +
                "|  7521 | WARD   | SALESMAN  | 7698 | 1981-02-22 |     30 | COMM              |       500.00 |\n" +
                "|  7521 | WARD   | SALESMAN  | 7698 | 1981-02-22 |     30 | SAL               |      1250.00 |\n" +
                "|  7566 | JONES  | MANAGER   | 7839 | 1981-02-04 |     20 | SAL               |      2975.00 |\n" +
                "|  7654 | MARTIN | SALESMAN  | 7698 | 1981-09-28 |     30 | COMM              |      1400.00 |\n" +
                "|  7654 | MARTIN | SALESMAN  | 7698 | 1981-09-28 |     30 | SAL               |      1250.00 |\n" +
                "|  7698 | BLAKE  | MANAGER   | 7839 | 1981-01-05 |     30 | SAL               |      2850.00 |\n" +
                "|  7782 | CLARK  | MANAGER   | 7839 | 1981-06-09 |     10 | SAL               |      2450.00 |\n" +
                "|  7788 | SCOTT  | ANALYST   | 7566 | 1987-04-19 |     20 | SAL               |      3000.00 |\n" +
                "|  7839 | KING   | PRESIDENT |      | 1981-11-17 |     10 | SAL               |      5000.00 |\n" +
                "|  7844 | TURNER | SALESMAN  | 7698 | 1981-09-08 |     30 | COMM              |         0.00 |\n" +
                "|  7844 | TURNER | SALESMAN  | 7698 | 1981-09-08 |     30 | SAL               |      1500.00 |\n" +
                "|  7876 | ADAMS  | CLERK     | 7788 | 1987-05-23 |     20 | SAL               |      1100.00 |\n" +
                "|  7900 | JAMES  | CLERK     | 7698 | 1981-12-03 |     30 | SAL               |       950.00 |\n" +
                "|  7902 | FORD   | ANALYST   | 7566 | 1981-12-03 |     20 | SAL               |      3000.00 |\n" +
                "|  7934 | MILLER | CLERK     | 7782 | 1982-01-23 |     10 | SAL               |      1300.00 |\n" +
                "+-------+--------+-----------+------+------------+--------+-------------------+--------------+\n" +
                "(18 rows)\n" +
                "\n" +
                "SELECT *\n" +
                "FROM emp\n" +
                "UNPIVOT INCLUDE NULLS (remuneration\n" +
                "  FOR remuneration_type IN (comm, sal));\n" +
                "+-------+--------+-----------+------+------------+--------+-------------------+--------------+\n" +
                "| EMPNO | ENAME  | JOB       | MGR  | HIREDATE   | DEPTNO | REMUNERATION_TYPE | REMUNERATION |\n" +
                "+-------+--------+-----------+------+------------+--------+-------------------+--------------+\n" +
                "|  7369 | SMITH  | CLERK     | 7902 | 1980-12-17 |     20 | COMM              |              |\n" +
                "|  7369 | SMITH  | CLERK     | 7902 | 1980-12-17 |     20 | SAL               |       800.00 |\n" +
                "|  7499 | ALLEN  | SALESMAN  | 7698 | 1981-02-20 |     30 | COMM              |       300.00 |\n" +
                "|  7499 | ALLEN  | SALESMAN  | 7698 | 1981-02-20 |     30 | SAL               |      1600.00 |\n" +
                "|  7521 | WARD   | SALESMAN  | 7698 | 1981-02-22 |     30 | COMM              |       500.00 |\n" +
                "|  7521 | WARD   | SALESMAN  | 7698 | 1981-02-22 |     30 | SAL               |      1250.00 |\n" +
                "|  7566 | JONES  | MANAGER   | 7839 | 1981-02-04 |     20 | COMM              |              |\n" +
                "|  7566 | JONES  | MANAGER   | 7839 | 1981-02-04 |     20 | SAL               |      2975.00 |\n" +
                "|  7654 | MARTIN | SALESMAN  | 7698 | 1981-09-28 |     30 | COMM              |      1400.00 |\n" +
                "|  7654 | MARTIN | SALESMAN  | 7698 | 1981-09-28 |     30 | SAL               |      1250.00 |\n" +
                "|  7698 | BLAKE  | MANAGER   | 7839 | 1981-01-05 |     30 | COMM              |              |\n" +
                "|  7698 | BLAKE  | MANAGER   | 7839 | 1981-01-05 |     30 | SAL               |      2850.00 |\n" +
                "|  7782 | CLARK  | MANAGER   | 7839 | 1981-06-09 |     10 | COMM              |              |\n" +
                "|  7782 | CLARK  | MANAGER   | 7839 | 1981-06-09 |     10 | SAL               |      2450.00 |\n" +
                "|  7788 | SCOTT  | ANALYST   | 7566 | 1987-04-19 |     20 | COMM              |              |\n" +
                "|  7788 | SCOTT  | ANALYST   | 7566 | 1987-04-19 |     20 | SAL               |      3000.00 |\n" +
                "|  7839 | KING   | PRESIDENT |      | 1981-11-17 |     10 | COMM              |              |\n" +
                "|  7839 | KING   | PRESIDENT |      | 1981-11-17 |     10 | SAL               |      5000.00 |\n" +
                "|  7844 | TURNER | SALESMAN  | 7698 | 1981-09-08 |     30 | COMM              |         0.00 |\n" +
                "|  7844 | TURNER | SALESMAN  | 7698 | 1981-09-08 |     30 | SAL               |      1500.00 |\n" +
                "|  7876 | ADAMS  | CLERK     | 7788 | 1987-05-23 |     20 | COMM              |              |\n" +
                "|  7876 | ADAMS  | CLERK     | 7788 | 1987-05-23 |     20 | SAL               |      1100.00 |\n" +
                "|  7900 | JAMES  | CLERK     | 7698 | 1981-12-03 |     30 | COMM              |              |\n" +
                "|  7900 | JAMES  | CLERK     | 7698 | 1981-12-03 |     30 | SAL               |       950.00 |\n" +
                "|  7902 | FORD   | ANALYST   | 7566 | 1981-12-03 |     20 | COMM              |              |\n" +
                "|  7902 | FORD   | ANALYST   | 7566 | 1981-12-03 |     20 | SAL               |      3000.00 |\n" +
                "|  7934 | MILLER | CLERK     | 7782 | 1982-01-23 |     10 | COMM              |              |\n" +
                "|  7934 | MILLER | CLERK     | 7782 | 1982-01-23 |     10 | SAL               |      1300.00 |\n" +
                "+-------+--------+-----------+------+------------+--------+-------------------+--------------+\n" +
                "(28 rows)\n" +
                "\n" +
                "SELECT *\n" +
                "FROM emp\n" +
                "UNPIVOT INCLUDE NULLS (remuneration\n" +
                "  FOR remuneration_type IN (comm, sal))\n" +
                "WHERE deptno = 20 AND remuneration > 500;\n" +
                "+-------+-------+---------+------+------------+--------+-------------------+--------------+\n" +
                "| EMPNO | ENAME | JOB     | MGR  | HIREDATE   | DEPTNO | REMUNERATION_TYPE | REMUNERATION |\n" +
                "+-------+-------+---------+------+------------+--------+-------------------+--------------+\n" +
                "|  7369 | SMITH | CLERK   | 7902 | 1980-12-17 |     20 | SAL               |       800.00 |\n" +
                "|  7566 | JONES | MANAGER | 7839 | 1981-02-04 |     20 | SAL               |      2975.00 |\n" +
                "|  7788 | SCOTT | ANALYST | 7566 | 1987-04-19 |     20 | SAL               |      3000.00 |\n" +
                "|  7876 | ADAMS | CLERK   | 7788 | 1987-05-23 |     20 | SAL               |      1100.00 |\n" +
                "|  7902 | FORD  | ANALYST | 7566 | 1981-12-03 |     20 | SAL               |      3000.00 |\n" +
                "+-------+-------+---------+------+------------+--------+-------------------+--------------+\n" +
                "(5 rows)\n" +
                "\n" +
                "SELECT deptno,\n" +
                "  SUM(remuneration) AS r,\n" +
                "  SUM(remuneration) FILTER (WHERE job = 'CLERK') AS cr\n" +
                "FROM emp\n" +
                "UNPIVOT INCLUDE NULLS (remuneration\n" +
                "  FOR remuneration_type IN (comm, sal))\n" +
                "GROUP BY deptno\n" +
                "HAVING COUNT(*) > 6\n" +
                "-- ORDER BY deptno" +
                ";\n" +
                "+--------+----------+---------+\n" +
                "| DEPTNO | R        | CR      |\n" +
                "+--------+----------+---------+\n" +
                "|     20 | 10875.00 | 1900.00 |\n" +
                "|     30 | 11600.00 |  950.00 |\n" +
                "+--------+----------+---------+\n" +
                "(2 rows)\n" +
                "\n" +
                "SELECT *\n" +
                "FROM emp\n" +
                "UNPIVOT (remuneration\n" +
                "  FOR sal IN (comm AS 'commission',\n" +
                "                sal as 'salary'));\n" +
                "+-------+--------+-----------+------+------------+--------+------------+--------------+\n" +
                "| EMPNO | ENAME  | JOB       | MGR  | HIREDATE   | DEPTNO | SAL        | REMUNERATION |\n" +
                "+-------+--------+-----------+------+------------+--------+------------+--------------+\n" +
                "|  7369 | SMITH  | CLERK     | 7902 | 1980-12-17 |     20 | salary     |       800.00 |\n" +
                "|  7499 | ALLEN  | SALESMAN  | 7698 | 1981-02-20 |     30 | commission |       300.00 |\n" +
                "|  7499 | ALLEN  | SALESMAN  | 7698 | 1981-02-20 |     30 | salary     |      1600.00 |\n" +
                "|  7521 | WARD   | SALESMAN  | 7698 | 1981-02-22 |     30 | commission |       500.00 |\n" +
                "|  7521 | WARD   | SALESMAN  | 7698 | 1981-02-22 |     30 | salary     |      1250.00 |\n" +
                "|  7566 | JONES  | MANAGER   | 7839 | 1981-02-04 |     20 | salary     |      2975.00 |\n" +
                "|  7654 | MARTIN | SALESMAN  | 7698 | 1981-09-28 |     30 | commission |      1400.00 |\n" +
                "|  7654 | MARTIN | SALESMAN  | 7698 | 1981-09-28 |     30 | salary     |      1250.00 |\n" +
                "|  7698 | BLAKE  | MANAGER   | 7839 | 1981-01-05 |     30 | salary     |      2850.00 |\n" +
                "|  7782 | CLARK  | MANAGER   | 7839 | 1981-06-09 |     10 | salary     |      2450.00 |\n" +
                "|  7788 | SCOTT  | ANALYST   | 7566 | 1987-04-19 |     20 | salary     |      3000.00 |\n" +
                "|  7839 | KING   | PRESIDENT |      | 1981-11-17 |     10 | salary     |      5000.00 |\n" +
                "|  7844 | TURNER | SALESMAN  | 7698 | 1981-09-08 |     30 | commission |         0.00 |\n" +
                "|  7844 | TURNER | SALESMAN  | 7698 | 1981-09-08 |     30 | salary     |      1500.00 |\n" +
                "|  7876 | ADAMS  | CLERK     | 7788 | 1987-05-23 |     20 | salary     |      1100.00 |\n" +
                "|  7900 | JAMES  | CLERK     | 7698 | 1981-12-03 |     30 | salary     |       950.00 |\n" +
                "|  7902 | FORD   | ANALYST   | 7566 | 1981-12-03 |     20 | salary     |      3000.00 |\n" +
                "|  7934 | MILLER | CLERK     | 7782 | 1982-01-23 |     10 | salary     |      1300.00 |\n" +
                "+-------+--------+-----------+------+------------+--------+------------+--------------+\n" +
                "(18 rows)\n" +
                "\n" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT *\n" +
                "  FROM (VALUES (0, 1, 2, 3, 4),\n" +
                "               (10, 11, 12, 13, 14))\n" +
                "          AS t (c0, c1, c2, c3, c4))\n" +
                "UNPIVOT ((m0, m1, m2)\n" +
                "    FOR (a0, a1)\n" +
                "     IN ((c1, c2, c3) as ('col1','col2'),\n" +
                "         (c2, c3, c4)));\n" +
                "+----+----------+----------+----+----+----+\n" +
                "| C0 | A0       | A1       | M0 | M1 | M2 |\n" +
                "+----+----------+----------+----+----+----+\n" +
                "|  0 | col1     | col2     |  1 |  2 |  3 |\n" +
                "|  0 | C2_C3_C4 | C2_C3_C4 |  2 |  3 |  4 |\n" +
                "| 10 | col1     | col2     | 11 | 12 | 13 |\n" +
                "| 10 | C2_C3_C4 | C2_C3_C4 | 12 | 13 | 14 |\n" +
                "+----+----------+----------+----+----+----+\n" +
                "(4 rows)");
    }
}
