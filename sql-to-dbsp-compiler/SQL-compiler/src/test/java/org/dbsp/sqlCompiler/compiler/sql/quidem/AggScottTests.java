package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Ignore;
import org.junit.Test;

// AggTests that use the Scott database
// https://github.com/apache/calcite/blob/main/core/src/test/resources/sql/agg.iq
public class AggScottTests extends ScottBaseTests {
    @Test
    public void testPairs() {
        this.qs("""
                select comm, (comm, comm) in ((500, 500), (300, 300), (0, 0)) from emp;
                 comm | in
                -----------
                 0    | true
                 300  | true
                 500  | true
                 1400 | false
                      |
                      |
                      |
                      |
                      |
                      |
                      |
                      |
                      |
                      |
                -----------
                (14 rows)""");
    }

    @Test @Ignore("Cannot decorrelate LATERAL join")
    public void testLateral() {
        this.qs("""
                SELECT deptno, ename
                FROM
                  (SELECT DISTINCT deptno FROM emp) t1,
                  LATERAL (
                    SELECT ename, sal
                    FROM emp
                    WHERE deptno IN (t1.deptno, t1.deptno)
                    AND   deptno = t1.deptno
                    ORDER BY sal
                    DESC LIMIT 3);
                 deptno | ename
                ----------------
                (0 rows)""");
    }

    @Test
    public void testGrouping() {
        this.qs("""
                -- GROUPING in SELECT clause of CUBE query
                select deptno, job, count(*) as c, grouping(deptno) as d,
                  grouping(job) j, grouping(deptno, job) as x
                from emp
                group by cube(deptno, job);
                +--------+-----------+----+---+---+---+
                | DEPTNO | JOB       | C  | D | J | X |
                +--------+-----------+----+---+---+---+
                |     10 | CLERK|       1 | 0 | 0 | 0 |
                |     10 | MANAGER|     1 | 0 | 0 | 0 |
                |     10 | PRESIDENT|   1 | 0 | 0 | 0 |
                |     10 |NULL       |  3 | 0 | 1 | 1 |
                |     20 | ANALYST|     2 | 0 | 0 | 0 |
                |     20 | CLERK|       2 | 0 | 0 | 0 |
                |     20 | MANAGER|     1 | 0 | 0 | 0 |
                |     20 |NULL       |  5 | 0 | 1 | 1 |
                |     30 | CLERK|       1 | 0 | 0 | 0 |
                |     30 | MANAGER|     1 | 0 | 0 | 0 |
                |     30 | SALESMAN|    4 | 0 | 0 | 0 |
                |     30 |NULL       |  6 | 0 | 1 | 1 |
                |        | ANALYST|     2 | 1 | 0 | 2 |
                |        | CLERK|       4 | 1 | 0 | 2 |
                |        | MANAGER|     3 | 1 | 0 | 2 |
                |        | PRESIDENT|   1 | 1 | 0 | 2 |
                |        | SALESMAN|    4 | 1 | 0 | 2 |
                |        |NULL       | 14 | 1 | 1 | 3 |
                +--------+-----------+----+---+---+---+
                (18 rows)""");
    }

    @Test
    public void testGrouping2() {
        this.qs("""
                select deptno, group_id() as g, count(*) as c
                from emp
                group by grouping sets (deptno, (), ());
                +--------+---+----+
                | DEPTNO | G | C  |
                +--------+---+----+
                |     10 | 0 |  3 |
                |     20 | 0 |  5 |
                |     30 | 0 |  6 |
                |        | 0 | 14 |
                |        | 1 | 14 |
                +--------+---+----+
                (5 rows)

                -- Degenerate case: GROUP_ID() without GROUPING SETS
                select group_id() as g
                from emp
                group by ();
                +---+
                | G |
                +---+
                | 0 |
                +---+
                (1 row)

                -- GROUP_ID() does not make a query into an aggregate query
                select group_id() as g, sum(3) as s3
                from emp;
                +---+----+
                | G | S3 |
                +---+----+
                | 0 | 42 |
                +---+----+
                (1 row)

                -- Extremely degenerate case: GROUP_ID on an empty table
                select group_id() as g, sum(3) as s3
                from emp
                where empno < 0;
                +---+----+
                | G | S3 |
                +---+----+
                | 0 |    |
                +---+----+
                (1 row)

                -- As above, explicit empty GROUP BY
                select group_id() as g
                from emp
                where empno < 0
                group by ();
                +---+
                | G |
                +---+
                | 0 |
                +---+
                (1 row)

                -- As above, non-empty GROUP BY
                select group_id() as g
                from emp
                where empno < 0
                group by deptno;
                +---+
                | G |
                +---+
                +---+
                (0 rows)

                -- From http://rwijk.blogspot.com/2008/12/groupid.html
                select deptno
                       , job
                       , empno
                       , ename
                       , sum(sal) sumsal
                       , case grouping_id(deptno,job,empno)
                           when 0 then 'grouped by deptno,job,empno,ename'
                           when 1 then 'grouped by deptno,job'
                           when 3 then 'grouped by deptno'
                           when 7 then 'grouped by ()'
                         end gr_text
                    from emp
                   group by rollup(deptno,job,(empno,ename))
                   order by deptno
                       , job
                       , empno;
                +--------+-----------+-------+--------+----------+-----------------------------------+
                | DEPTNO | JOB       | EMPNO | ENAME  | SUMSAL   | GR_TEXT                           |
                +--------+-----------+-------+--------+----------+-----------------------------------+
                |     10 | CLERK|       7934 | MILLER|   1300.00 | grouped by deptno,job,empno,ename|
                |     10 | CLERK|            |NULL    |  1300.00 | grouped by deptno,job|
                |     10 | MANAGER|     7782 | CLARK|    2450.00 | grouped by deptno,job,empno,ename|
                |     10 | MANAGER|          |NULL    |  2450.00 | grouped by deptno,job|
                |     10 | PRESIDENT|   7839 | KING|     5000.00 | grouped by deptno,job,empno,ename|
                |     10 | PRESIDENT|        |NULL    |  5000.00 | grouped by deptno,job|
                |     10 |NULL       |       |NULL    |  8750.00 | grouped by deptno|
                |     20 | ANALYST|     7788 | SCOTT|    3000.00 | grouped by deptno,job,empno,ename|
                |     20 | ANALYST|     7902 | FORD|     3000.00 | grouped by deptno,job,empno,ename|
                |     20 | ANALYST|          |NULL    |  6000.00 | grouped by deptno,job|
                |     20 | CLERK|       7369 | SMITH|     800.00 | grouped by deptno,job,empno,ename|
                |     20 | CLERK|       7876 | ADAMS|    1100.00 | grouped by deptno,job,empno,ename|
                |     20 | CLERK|            |NULL    |  1900.00 | grouped by deptno,job|
                |     20 | MANAGER|     7566 | JONES|    2975.00 | grouped by deptno,job,empno,ename|
                |     20 | MANAGER|          |NULL    |  2975.00 | grouped by deptno,job|
                |     20 |NULL       |       |NULL    | 10875.00 | grouped by deptno|
                |     30 | CLERK|       7900 | JAMES|     950.00 | grouped by deptno,job,empno,ename|
                |     30 | CLERK|            |NULL    |   950.00 | grouped by deptno,job|
                |     30 | MANAGER|     7698 | BLAKE|    2850.00 | grouped by deptno,job,empno,ename|
                |     30 | MANAGER|          |NULL    |  2850.00 | grouped by deptno,job|
                |     30 | SALESMAN|    7499 | ALLEN|    1600.00 | grouped by deptno,job,empno,ename|
                |     30 | SALESMAN|    7521 | WARD|     1250.00 | grouped by deptno,job,empno,ename|
                |     30 | SALESMAN|    7654 | MARTIN|   1250.00 | grouped by deptno,job,empno,ename|
                |     30 | SALESMAN|    7844 | TURNER|   1500.00 | grouped by deptno,job,empno,ename|
                |     30 | SALESMAN|         |NULL    |  5600.00 | grouped by deptno,job|
                |     30 |NULL       |       |NULL    |  9400.00 | grouped by deptno|
                |        |NULL       |       |NULL    | 29025.00 | grouped by ()|
                +--------+-----------+-------+--------+----------+-----------------------------------+
                (27 rows)

                -- From http://rwijk.blogspot.com/2008/12/groupid.html
                select deptno
                       , job
                       , empno
                       , ename
                       , sum(sal) sumsal
                       , case grouping_id(deptno,job,empno)
                           when 0 then 'grouped by deptno,job,empno,ename'
                           when 1 then 'grouped by deptno,job'
                           when 3 then 'grouped by deptno, grouping set ' || cast(3+group_id() as varchar)
                           when 7 then 'grouped by (), grouping set ' || cast(5+group_id() as varchar)
                         end gr_text
                    from emp
                   group by grouping sets
                         ( (deptno,job,empno,ename)
                         , (deptno,job)
                         , deptno
                         , deptno
                         , ()
                         , ()
                         )
                   order by deptno
                       , job
                       , empno;
                +--------+-----------+-------+--------+----------+-----------------------------------+
                | DEPTNO | JOB       | EMPNO | ENAME  | SUMSAL   | GR_TEXT                           |
                +--------+-----------+-------+--------+----------+-----------------------------------+
                |     10 | CLERK|       7934 | MILLER|   1300.00 | grouped by deptno,job,empno,ename|
                |     10 | CLERK|            |NULL    |  1300.00 | grouped by deptno,job|
                |     10 | MANAGER|     7782 | CLARK|    2450.00 | grouped by deptno,job,empno,ename|
                |     10 | MANAGER|          |NULL    |  2450.00 | grouped by deptno,job|
                |     10 | PRESIDENT|   7839 | KING|     5000.00 | grouped by deptno,job,empno,ename|
                |     10 | PRESIDENT|        |NULL    |  5000.00 | grouped by deptno,job|
                |     10 |NULL       |       |NULL    |  8750.00 | grouped by deptno, grouping set 3|
                |     10 |NULL       |       |NULL    |  8750.00 | grouped by deptno, grouping set 4|
                |     20 | ANALYST|     7788 | SCOTT|    3000.00 | grouped by deptno,job,empno,ename|
                |     20 | ANALYST|     7902 | FORD|     3000.00 | grouped by deptno,job,empno,ename|
                |     20 | ANALYST|          |NULL    |  6000.00 | grouped by deptno,job|
                |     20 | CLERK|      7369 | SMITH|      800.00 | grouped by deptno,job,empno,ename|
                |     20 | CLERK|      7876 | ADAMS|     1100.00 | grouped by deptno,job,empno,ename|
                |     20 | CLERK|           |NULL     |  1900.00 | grouped by deptno,job|
                |     20 | MANAGER|     7566 | JONES|    2975.00 | grouped by deptno,job,empno,ename|
                |     20 | MANAGER|          |NULL    |  2975.00 | grouped by deptno,job|
                |     20 |NULL       |       |NULL    | 10875.00 | grouped by deptno, grouping set 3|
                |     20 |NULL       |       |NULL    | 10875.00 | grouped by deptno, grouping set 4|
                |     30 | CLERK|       7900 | JAMES|     950.00 | grouped by deptno,job,empno,ename|
                |     30 | CLERK|            |NULL    |   950.00 | grouped by deptno,job|
                |     30 | MANAGER|     7698 | BLAKE|    2850.00 | grouped by deptno,job,empno,ename|
                |     30 | MANAGER|          |NULL    |  2850.00 | grouped by deptno,job|
                |     30 | SALESMAN|    7499 | ALLEN|    1600.00 | grouped by deptno,job,empno,ename|
                |     30 | SALESMAN|    7521 | WARD|     1250.00 | grouped by deptno,job,empno,ename|
                |     30 | SALESMAN|    7654 | MARTIN|   1250.00 | grouped by deptno,job,empno,ename|
                |     30 | SALESMAN|    7844 | TURNER|   1500.00 | grouped by deptno,job,empno,ename|
                |     30 | SALESMAN|         |NULL    |  5600.00 | grouped by deptno,job|
                |     30 |NULL       |       |NULL    |  9400.00 | grouped by deptno, grouping set 3|
                |     30 |NULL       |       |NULL    |  9400.00 | grouped by deptno, grouping set 4|
                |        |NULL       |       |NULL    | 29025.00 | grouped by (), grouping set 5|
                |        |NULL       |       |NULL    | 29025.00 | grouped by (), grouping set 6|
                +--------+-----------+-------+--------+----------+-----------------------------------+
                (31 rows)

                -- There are duplicate GROUPING SETS
                select deptno, sum(sal) as s
                from emp as t
                group by grouping sets (deptno, deptno);
                +--------+----------+
                | DEPTNO | S        |
                +--------+----------+
                |     10 |  8750.00 |
                |     10 |  8750.00 |
                |     20 | 10875.00 |
                |     20 | 10875.00 |
                |     30 |  9400.00 |
                |     30 |  9400.00 |
                +--------+----------+
                (6 rows)

                -- Similar, not duplicate GROUPING SETS
                select deptno, sum(sal) as s
                from emp as t
                group by grouping sets (deptno);
                +--------+----------+
                | DEPTNO | S        |
                +--------+----------+
                |     10 |  8750.00 |
                |     20 | 10875.00 |
                |     30 |  9400.00 |
                +--------+----------+
                (3 rows)

                -- Complex GROUPING SETS clause that contains duplicates
                select sum(sal) as s
                from emp as t
                group by job,
                    grouping sets ( deptno,
                    grouping sets ( (deptno, comm is null), comm is null),
                         (comm is null)),
                    ();
                +---------+
                | S       |
                +---------+
                | 1300.00 |
                | 1300.00 |
                | 2450.00 |
                | 2450.00 |
                | 2850.00 |
                | 2850.00 |
                | 2975.00 |
                | 2975.00 |
                | 5000.00 |
                | 5000.00 |
                | 5000.00 |
                | 5000.00 |
                | 6000.00 |
                |  950.00 |
                |  950.00 |
                | 1900.00 |
                | 1900.00 |
                | 4150.00 |
                | 4150.00 |
                | 5600.00 |
                | 5600.00 |
                | 5600.00 |
                | 5600.00 |
                | 6000.00 |
                | 6000.00 |
                | 6000.00 |
                | 8275.00 |
                | 8275.00 |
                +---------+
                (28 rows)

                -- Equivalent query using flat GROUPING SETS
                select sum(sal) as s
                from emp
                group by grouping sets ((job, deptno, comm is null),
                  (job, deptno), (job, comm is null), (job, comm is null));
                +---------+
                | S       |
                +---------+
                | 1300.00 |
                | 1300.00 |
                | 2450.00 |
                | 2450.00 |
                | 2850.00 |
                | 2850.00 |
                | 2975.00 |
                | 2975.00 |
                | 5000.00 |
                | 5000.00 |
                | 5000.00 |
                | 5000.00 |
                | 6000.00 |
                |  950.00 |
                |  950.00 |
                | 1900.00 |
                | 1900.00 |
                | 4150.00 |
                | 4150.00 |
                | 5600.00 |
                | 5600.00 |
                | 5600.00 |
                | 5600.00 |
                | 6000.00 |
                | 6000.00 |
                | 6000.00 |
                | 8275.00 |
                | 8275.00 |
                +---------+
                (28 rows)""");
    }

    @Test
    public void testComplexGrouping() {
        // Had to modify the output from the Calcite test, it looks
        // like Calcite is wrong, Postgres seems to agree.
        this.qs("""
                  -- Equivalent query, but with GROUP_ID and GROUPING_ID
                  select deptno, comm is null, job, sum(sal) as s,
                    grouping_id(job, deptno, comm is null) as g,
                    group_id() as i
                  from emp
                  group by grouping sets ((job, deptno, comm is null),
                    (job, deptno), (job, comm is null), (job, comm is null))
                  order by g, i, s desc;
                  +----+---+-----------+---------+---+---+
                  |  D | C |         J | S       | G | I |
                  +----+---+-----------+---------+---+---+
                  | 20 | T | ANALYST|    6000.00 | 0 | 0 |
                  | 30 | F | SALESMAN|   5600.00 | 0 | 0 |
                  | 10 | T | PRESIDENT|  5000.00 | 0 | 0 |
                  | 20 | T | MANAGER|    2975.00 | 0 | 0 |
                  | 30 | T | MANAGER|    2850.00 | 0 | 0 |
                  | 10 | T | MANAGER|    2450.00 | 0 | 0 |
                  | 20 | T | CLERK|      1900.00 | 0 | 0 |
                  | 10 | T | CLERK|      1300.00 | 0 | 0 |
                  | 30 | T | CLERK|       950.00 | 0 | 0 |
                  |    | T | MANAGER|    8275.00 | 2 | 1 |
                  |    | T | ANALYST|    6000.00 | 2 | 1 |
                  |    | F | SALESMAN|   5600.00 | 2 | 1 |
                  |    | T | PRESIDENT|  5000.00 | 2 | 1 |
                  |    | T | CLERK|      4150.00 | 2 | 1 |
                  | 20 |   | ANALYST|    6000.00 | 1 | 0 |
                  | 30 |   | SALESMAN|   5600.00 | 1 | 0 |
                  | 10 |   | PRESIDENT|  5000.00 | 1 | 0 |
                  | 20 |   | MANAGER|    2975.00 | 1 | 0 |
                  | 30 |   | MANAGER|    2850.00 | 1 | 0 |
                  | 10 |   | MANAGER|    2450.00 | 1 | 0 |
                  | 20 |   | CLERK|      1900.00 | 1 | 0 |
                  | 10 |   | CLERK|      1300.00 | 1 | 0 |
                  | 30 |   | CLERK|       950.00 | 1 | 0 |
                  |    | T | MANAGER|    8275.00 | 2 | 0 |
                  |    | T | ANALYST|    6000.00 | 2 | 0 |
                  |    | F | SALESMAN|   5600.00 | 2 | 0 |
                  |    | T | PRESIDENT|  5000.00 | 2 | 0 |
                  |    | T | CLERK|      4150.00 | 2 | 0 |
                  +----+---+-----------+---------+---+---+
                  (28 rows)""");
    }

    @Test
    public void testSimple() {
        this.qs("""
                  -- [KYLIN-751] Max on negative double values is not working
                  -- [CALCITE-735] Primitive.DOUBLE.min should be large and negative
                  select max(v) as x, min(v) as n
                  from (values cast(-86.4 as double), cast(-100 as double)) as t(v);
                  +-------+--------+
                  | X     | N      |
                  +-------+--------+
                  | -86.4 | -100.0 |
                  +-------+--------+
                  (1 row)

                  select max(v) as x, min(v) as n
                  from (values cast(-86.4 as double), cast(-100 as double), cast(2 as double)) as t(v);
                  +-----+--------+
                  | X   | N      |
                  +-----+--------+
                  | 2.0 | -100.0 |
                  +-----+--------+
                  (1 row)

                  select max(v) as x, min(v) as n
                  from (values cast(-86.4 as real), cast(-100 as real)) as t(v);
                  +-------+--------+
                  | X     | N      |
                  +-------+--------+
                  | -86.4 | -100.0 |
                  +-------+--------+
                  (1 row)

                  -- [CALCITE-551] Sub-query inside aggregate function
                  SELECT SUM(
                    CASE WHEN deptno IN (SELECT deptno FROM dept) THEN 1
                    ELSE 0 END) as s
                  FROM emp;
                  +----+
                  | S  |
                  +----+
                  | 14 |
                  +----+
                  (1 row)

                  SELECT SUM((select min(cast(deptno as integer)) from dept)) as s
                  FROM emp;
                  +-----+
                  | S   |
                  +-----+
                  | 140 |
                  +-----+
                  (1 row)

                  -- As above, but with GROUP BY
                  SELECT SUM((select min(cast(deptno as integer)) from dept)) as s, deptno
                  FROM emp
                  GROUP BY deptno;
                  +----+--------+
                  | S  | DEPTNO |
                  +----+--------+
                  | 30 |     10 |
                  | 50 |     20 |
                  | 60 |     30 |
                  +----+--------+
                  (3 rows)

                  -- As above, but with correlation
                  SELECT SUM(
                    (select char_length(dname) from dept where dept.deptno = emp.empno)) as s
                  FROM emp;
                  +---+
                  | S |
                  +---+
                  |   |
                  +---+
                  (1 row)""");
    }

    @Test @Ignore("fusion, collect not yet implemented")
    public void testFusion() {
        this.qs("""
                -- FUSION rolled up using CARDINALITY
                select cardinality(fusion(empnos)) as f_empnos_length
                from (
                  select deptno, collect(empno) as empnos
                  from emp
                  group by deptno);
                +-----------------+
                | F_EMPNOS_LENGTH |
                +-----------------+
                |              14 |
                +-----------------+
                (1 row)

                -- FUSION
                select cardinality(fusion(empnos)) as f_empnos_length from (select deptno, collect(empno) as empnos
                from emp
                group by deptno);
                +-----------------+
                | F_EMPNOS_LENGTH |
                +-----------------+
                |              14 |
                +-----------------+
                (1 row)

                -- FUSION on sub-total
                select job, fusion(empnos) as empnos
                from (
                  select job, collect(empno) as empnos
                  from emp
                  group by deptno, job)
                group by job;
                +-----------+--------------------------+
                | JOB       | EMPNOS                   |
                +-----------+--------------------------+
                | ANALYST   | [7788, 7902]             |
                | CLERK     | [7934, 7369, 7876, 7900] |
                | MANAGER   | [7782, 7566, 7698]       |
                | PRESIDENT | [7839]                   |
                | SALESMAN  | [7499, 7521, 7654, 7844] |
                +-----------+--------------------------+
                (5 rows)

                -- FUSION grand total
                select fusion(deptnos) as deptnos
                from (
                  select collect(distinct deptno) as deptnos
                  from emp
                  group by deptno);
                +--------------+
                | DEPTNOS      |
                +--------------+
                | [20, 10, 30] |
                +--------------+
                (1 row)

                -- COLLECT
                select deptno, collect(empno) as empnos
                from emp
                group by deptno;
                +--------+--------------------------------------+
                | DEPTNO | EMPNOS                               |
                +--------+--------------------------------------+
                |     10 | [7782, 7839, 7934]                   |
                |     20 | [7369, 7566, 7788, 7876, 7902]       |
                |     30 | [7499, 7521, 7654, 7698, 7844, 7900] |
                +--------+--------------------------------------+
                (3 rows)

                -- COLLECT DISTINCT
                select deptno, collect(distinct job) as jobs
                from emp
                group by deptno;
                +--------+-----------------------------+
                | DEPTNO | JOBS                        |
                +--------+-----------------------------+
                |     10 | [MANAGER, CLERK, PRESIDENT] |
                |     20 | [CLERK, ANALYST, MANAGER]   |
                |     30 | [SALESMAN, MANAGER, CLERK]  |
                +--------+-----------------------------+
                (3 rows)

                -- COLLECT ... FILTER
                select deptno, collect(empno) filter (where empno < 7550) as empnos
                from emp
                group by deptno;
                +--------+--------------+
                | DEPTNO | EMPNOS       |
                +--------+--------------+
                |     10 | []           |
                |     20 | [7369]       |
                |     30 | [7499, 7521] |
                +--------+--------------+
                (3 rows)""");
    }

    @Test
    public void testConditionalAggregate() {
        this.qs("""
                -- Aggregate FILTER
                select deptno,
                  sum(sal) filter (where job = 'CLERK') c_sal,
                  sum(sal) filter (where job = 'CLERK' and deptno > 10) c10_sal,
                  max(sal) filter (where job = 'CLERK') as max_c,
                  min(sal) filter (where job = 'CLERK') as min_c,
                  max(sal) filter (where job = 'CLERK')
                    - min(sal) filter (where job = 'CLERK') as range_c,
                  max(sal) filter (where job = 'SALESMAN')
                    - min(sal) filter (where job = 'SALESMAN') as range_m
                from emp
                group by deptno;
                +--------+---------+---------+---------+---------+---------+---------+
                | DEPTNO | C_SAL   | C10_SAL | MAX_C   | MIN_C   | RANGE_C | RANGE_M |
                +--------+---------+---------+---------+---------+---------+---------+
                |     10 | 1300.00 |         | 1300.00 | 1300.00 |    0.00 |         |
                |     20 | 1900.00 | 1900.00 | 1100.00 |  800.00 |  300.00 |         |
                |     30 |  950.00 |  950.00 |  950.00 |  950.00 |    0.00 |  350.00 |
                +--------+---------+---------+---------+---------+---------+---------+
                (3 rows)

                -- Aggregate FILTER on condition in GROUP BY
                select deptno,
                  sum(sal) filter (where deptno = 10) sal_10
                from emp
                group by deptno;
                +--------+---------+
                | DEPTNO | SAL_10  |
                +--------+---------+
                |     10 | 8750.00 |
                |     20 |         |
                |     30 |         |
                +--------+---------+
                (3 rows)

                -- Aggregate FILTER with HAVING
                select deptno
                from emp
                group by deptno
                having sum(sal) filter (where job = 'CLERK') > 1000;
                +--------+
                | DEPTNO |
                +--------+
                |     10 |
                |     20 |
                +--------+
                (2 rows)""");
    }

    // Test case for issue 1508: query with order by that does not
    // select the ordered field is compiled incorrectly
    @Test
    public void testOrderByFilter() {
        this.qs("""
                -- Aggregate FILTER with ORDER BY
                select deptno
                from emp
                group by deptno
                order by sum(sal) filter (where job = 'CLERK');
                +--------+
                | DEPTNO |
                +--------+
                |     30 |
                |     10 |
                |     20 |
                +--------+
                (3 rows)""");
    }

    @Test
    public void testAggregates() {
        this.qs("""
                -- Aggregate FILTER with JOIN
                select dept.deptno,
                  sum(sal) filter (where 1 < 2) as s,
                  sum(sal) as s1,
                  count(*) filter (where emp.ename < dept.dname) as c
                from emp
                join dept using (deptno)
                group by dept.deptno;
                +--------+----------+----------+---+
                | DEPTNO | S        | S1       | C |
                +--------+----------+----------+---+
                |     10 |  8750.00 |  8750.00 | 0 |
                |     20 | 10875.00 | 10875.00 | 3 |
                |     30 |  9400.00 |  9400.00 | 4 |
                +--------+----------+----------+---+
                (3 rows)

                -- Aggregate FILTER with DISTINCT
                select deptno,
                 count(distinct job) as cdj
                from emp
                group by deptno;
                +--------+-----+
                | DEPTNO | CDJ |
                +--------+-----+
                |     10 |   3 |
                |     20 |   3 |
                |     30 |   3 |
                +--------+-----+
                (3 rows)

                select deptno,
                 count(distinct job) filter (where job <> 'SALESMAN') as cdj
                from emp
                group by deptno;
                +--------+-----+
                | DEPTNO | CDJ |
                +--------+-----+
                |     10 |   3 |
                |     20 |   3 |
                |     30 |   2 |
                +--------+-----+
                (3 rows)

                -- Convert CASE to FILTER
                select count(case x when 0 then null else -1 end) as c
                from (values 0, null, 0, 1) as t(x);
                +---+
                | C |
                +---+
                | 2 |
                +---+
                (1 row)

                -- Convert CASE to FILTER with no input rows
                select  COALESCE(sum(case when x = 1 then 1 else 0 end),0) as a,
                        COALESCE(sum(case when x = 1 then 2 else 0 end),0) as b,
                        COALESCE(sum(case when x = 1 then 3 else -1 end),0) as c
                from (values 0, null, 0, 2) as t(x) where x*x=1;
                +---+---+---+
                | A | B | C |
                +---+---+---+
                | 0 | 0 | 0 |
                +---+---+---+
                (1 row)

                -- Convert CASE to FILTER with no input rows
                select  sum(case when x = 1 then 1 else 0 end) as a,
                        sum(case when x = 1 then 2 else 0 end) as b,
                        sum(case when x = 1 then 3 else -1 end) as c
                from (values 0, null, 0, 2) as t(x) where x*x=1;
                +---+---+---+
                | A | B | C |
                +---+---+---+
                |   |   |   |
                +---+---+---+
                (1 row)

                -- Convert CASE to FILTER without matches
                select  sum(case when x = 1 then 1 else 0 end) as a,
                        sum(case when x = 1 then 2 else 0 end) as b,
                        sum(case when x = 1 then 3 else -1 end) as c
                from (values 0, null, 0, 2) as t(x);
                +---+---+----+
                | A | B | C  |
                +---+---+----+
                | 0 | 0 | -4 |
                +---+---+----+
                (1 row)

                -- Same, expressed as FILTER
                select count(*) filter (where (x = 0) is not true) as c
                from (values 0, null, 0, 1) as t(x);
                +---+
                | C |
                +---+
                | 2 |
                +---+
                (1 row)

                -- Similar, not quite the same
                select count(*) filter (where (x = 0) is false) as c
                from (values 0, null, 0, 1) as t(x);
                +---+
                | C |
                +---+
                | 1 |
                +---+
                (1 row)""");
    }

    @Test
    public void testCompositeCount() {
        this.qs("""
                -- Composite COUNT and FILTER
                select count(*) as c,
                  count(*) filter (where z > 1) as cf,
                  count(x) as cx,
                  count(x) filter (where z > 1) as cxf,
                  count(x, y) as cxy,
                  count(x, y) filter (where z > 1) as cxyf
                from (values (1, 1, 1), (2, 2, 2), (3, null, 3), (null, 4, 4)) as t(x, y, z);
                +---+----+----+-----+-----+------+
                | C | CF | CX | CXF | CXY | CXYF |
                +---+----+----+-----+-----+------+
                | 4 |  3 |  3 |   2 |   2 |    1 |
                +---+----+----+-----+-----+------+
                (1 row)""");
    }

    @Test
    public void testAggregates2() {
        this.qs("""
                select count(distinct deptno) as cd, count(*) as c
                from emp
                group by deptno;
                +----+---+
                | CD | C |
                +----+---+
                |  1 | 3 |
                |  1 | 5 |
                |  1 | 6 |
                +----+---+
                (3 rows)""");
    }

    @Test
    public void testDistinctAggregates() {
        this.qs("""
                select deptno, count(distinct deptno) as c
                from emp
                group by deptno;
                +--------+---+
                | DEPTNO | C |
                +--------+---+
                |     10 | 1 |
                |     20 | 1 |
                |     30 | 1 |
                +--------+---+
                (3 rows)

                select count(distinct deptno) as c
                from emp
                group by deptno;
                +---+
                | C |
                +---+
                | 1 |
                | 1 |
                | 1 |
                +---+
                (3 rows)""");
    }

    @Test
    public void testCubeDistinct() {
        this.qs("""
                select count(distinct deptno) as cd, count(*) as c
                from emp
                group by cube(deptno);
                +----+---+
                | CD | C |
                +----+---+
                |  1 | 3 |
                |  1 | 5 |
                |  1 | 6 |
                |  3 | 3 |
                +----+---+
                (4 rows)""");
    }

    @Test
    public void testDistinctCount() {
        // These tests cannot be run without optimizations.
        this.qs("""
                -- Multiple distinct count and non-distinct aggregates
                select deptno,
                 count(distinct job) as dj,
                 count(job) as j,
                 count(distinct mgr) as m,
                 sum(sal) as s
                from emp
                group by deptno;
                +--------+----+---+---+----------+
                | DEPTNO | DJ | J | M | S        |
                +--------+----+---+---+----------+
                |     10 |  3 | 3 | 2 |  8750.00 |
                |     20 |  3 | 5 | 4 | 10875.00 |
                |     30 |  3 | 6 | 2 |  9400.00 |
                +--------+----+---+---+----------+
                (3 rows)

                -- Multiple distinct count
                select deptno,
                 count(distinct job) as j, count(distinct mgr) as m
                from emp
                group by deptno;
                +--------+---+---+
                | DEPTNO | J | M |
                +--------+---+---+
                |     10 | 3 | 2 |
                |     20 | 3 | 4 |
                |     30 | 3 | 2 |
                +--------+---+---+
                (3 rows)

                -- Multiple distinct count and non-distinct aggregates, no GROUP BY
                select count(distinct job) as dj,
                 count(job) as j,
                 count(distinct mgr) as m,
                 sum(sal) as s
                from emp;
                +----+----+---+----------+
                | DJ | J  | M | S        |
                +----+----+---+----------+
                |  5 | 14 | 6 | 29025.00 |
                +----+----+---+----------+
                (1 row)""");
    }

    @Test
    public void testAvg() {
        this.qs("""
                select avg(comm) as a, count(comm) as c from
                emp where empno < 7844;
                +-------------------+---+
                | A                 | C |
                +-------------------+---+
                | 733.3333333333333 | 3 |
                +-------------------+---+
                (1 row)""");
    }

    @Test
    public void testAggregates3() {
        this.qs("""
                -- [CALCITE-846] Push aggregate with FILTER through UNION ALL
                select deptno, count(*) filter (where job = 'CLERK') as cf, count(*) as c
                from (
                  select * from emp where deptno < 20
                  union all
                  select * from emp where deptno > 20)
                group by deptno;
                +--------+----+---+
                | DEPTNO | CF | C |
                +--------+----+---+
                |     10 |  1 | 3 |
                |     30 |  1 | 6 |
                +--------+----+---+
                (2 rows)

                -- [CALCITE-751] Aggregate join transpose
                select count(*)
                from emp join dept using (deptno);
                +--------+
                | EXPR$0 |
                +--------+
                |     14 |
                +--------+
                (1 row)

                -- Push sum: splits into sum * count
                select sum(sal)
                from emp join dept using (deptno);
                +----------+
                | EXPR$0   |
                +----------+
                | 29025.00 |
                +----------+
                (1 row)

                -- Push sum; no aggregate needed after join
                select sum(sal)
                from emp join dept using (deptno)
                group by emp.deptno, dept.deptno;
                +----------+
                | EXPR$0   |
                +----------+
                | 10875.00 |
                |  8750.00 |
                |  9400.00 |
                +----------+
                (3 rows)

                -- Push sum; group by only one of the join keys
                select sum(sal)
                from emp join dept using (deptno)
                group by emp.deptno;
                +----------+
                | EXPR$0   |
                +----------+
                | 10875.00 |
                |  8750.00 |
                |  9400.00 |
                +----------+
                (3 rows)

                -- Push min; Join-Aggregate is optimized to SemiJoin
                select min(sal)
                from emp join dept using (deptno)
                group by emp.deptno;
                +---------+
                | EXPR$0  |
                +---------+
                | 1300.00 |
                |  800.00 |
                |  950.00 |
                +---------+
                (3 rows)

                -- Push sum and count
                select count(*) as c, sum(sal) as s
                from emp join dept using (deptno);
                +----+----------+
                | C  | S        |
                +----+----------+
                | 14 | 29025.00 |
                +----+----------+
                (1 row)

                -- Push sum and count, group by join key
                select count(*) as c, sum(sal) as s
                from emp join dept using (deptno) group by emp.deptno;
                +---+----------+
                | C | S        |
                +---+----------+
                | 3 |  8750.00 |
                | 5 | 10875.00 |
                | 6 |  9400.00 |
                +---+----------+
                (3 rows)

                -- Push sum and count, group by join key plus another column
                select count(*) as c, sum(sal) as s
                from emp join dept using (deptno) group by emp.job, dept.deptno;
                +---+---------+
                | C | S       |
                +---+---------+
                | 1 | 1300.00 |
                | 1 | 2450.00 |
                | 1 | 2850.00 |
                | 1 | 2975.00 |
                | 1 | 5000.00 |
                | 1 |  950.00 |
                | 2 | 1900.00 |
                | 2 | 6000.00 |
                | 4 | 5600.00 |
                +---+---------+
                (9 rows)

                -- Push sum and count, group by non-join column
                select count(*) as c, sum(sal) as s
                from emp join dept using (deptno) group by emp.job;
                +---+---------+
                | C | S       |
                +---+---------+
                | 1 | 5000.00 |
                | 2 | 6000.00 |
                | 3 | 8275.00 |
                | 4 | 4150.00 |
                | 4 | 5600.00 |
                +---+---------+
                (5 rows)

                -- Push count and sum, group by superset of join key
                select count(*) as c, sum(sal) as s
                from emp join dept using (deptno) group by emp.job, dept.deptno;
                +---+---------+
                | C | S       |
                +---+---------+
                | 1 | 5000.00 |
                | 2 | 6000.00 |
                | 4 | 5600.00 |
                | 1 | 1300.00 |
                | 1 | 2450.00 |
                | 1 | 2850.00 |
                | 1 | 2975.00 |
                | 1 |  950.00 |
                | 2 | 1900.00 |
                +---+---------+
                (9 rows)

                -- Push count and sum, group by a column being aggregated
                select count(*) as c, sum(sal) as s
                from emp join dept using (deptno) group by emp.sal;
                +---+---------+
                | C | S       |
                +---+---------+
                | 1 | 5000.00 |
                | 2 | 6000.00 |
                | 1 | 1100.00 |
                | 1 | 1300.00 |
                | 1 | 1500.00 |
                | 1 | 1600.00 |
                | 1 | 2450.00 |
                | 1 | 2850.00 |
                | 1 | 2975.00 |
                | 1 |  800.00 |
                | 1 |  950.00 |
                | 2 | 2500.00 |
                +---+---------+
                (12 rows)

                -- Push sum, self-join, returning one row with a null value
                select sum(e.sal) as s
                from emp e join emp m on e.mgr = e.empno;
                +---+
                | S |
                +---+
                |   |
                +---+
                (1 row)

                -- Push sum, self-join
                select sum(e.sal) as s
                from emp e join emp m on e.mgr = m.empno;
                +----------+
                | S        |
                +----------+
                | 24025.00 |
                +----------+
                (1 row)

                -- Push sum, self-join, cartesian product over nullable and non-nullable columns
                select sum(e.sal) as ss, count(e.sal) as cs, count(e.mgr) as cm
                from emp e
                join emp m on e.deptno = m.deptno
                group by e.deptno, m.deptno;
                +----------+----+----+
                | SS       | CS | CM |
                +----------+----+----+
                | 26250.00 |  9 |  6 |
                | 54375.00 | 25 | 25 |
                | 56400.00 | 36 | 36 |
                +----------+----+----+
                (3 rows)

                -- Push sum, self-join, aggregate by column on "many" side
                select sum(e.sal) as s
                from emp e join emp m on e.mgr = m.empno
                group by m.empno;
                +---------+
                | S       |
                +---------+
                | 1100.00 |
                | 1300.00 |
                | 6000.00 |
                | 6550.00 |
                |  800.00 |
                | 8275.00 |
                +---------+
                (6 rows)

                -- Push sum, self-join, aggregate by column on "one" side.
                -- Note inflated totals due to cartesian product.
                select sum(m.sal) as s
                from emp e join emp m on e.mgr = m.empno
                group by m.empno;
                +----------+
                | S        |
                +----------+
                | 14250.00 |
                | 15000.00 |
                |  2450.00 |
                |  3000.00 |
                |  3000.00 |
                |  5950.00 |
                +----------+
                (6 rows)""");
    }

    @Test
    public void testNestedOrderby() {
        this.qs("""
                -- Collation of LogicalAggregate ([CALCITE-783] and [CALCITE-822])
                select  sum(x) as sum_cnt,
                  count(distinct y) as cnt_dist
                from
                  (
                  select
                    count(*) as x,
                          t1.job      as y,
                    t1.deptno as z
                  from
                    emp t1
                  group by t1.job, t1.deptno
                  order by t1.job, t1.deptno
                ) sq(x,y,z)
                group by z
                order by sum_cnt;
                +---------+----------+
                | SUM_CNT | CNT_DIST |
                +---------+----------+
                |       3 |        3 |
                |       5 |        3 |
                |       6 |        3 |
                +---------+----------+
                (3 rows)""");
    }

    @Test
    public void testAggregates4() {
        this.qs("""
                -- [CALCITE-938] Aggregate row count
                select empno, d.deptno
                from emp
                join (select distinct deptno from dept) d
                using (deptno);
                +-------+--------+
                | EMPNO | DEPTNO |
                +-------+--------+
                |  7369 |     20 |
                |  7499 |     30 |
                |  7521 |     30 |
                |  7566 |     20 |
                |  7654 |     30 |
                |  7698 |     30 |
                |  7782 |     10 |
                |  7788 |     20 |
                |  7839 |     10 |
                |  7844 |     30 |
                |  7876 |     20 |
                |  7900 |     30 |
                |  7902 |     20 |
                |  7934 |     10 |
                +-------+--------+
                (14 rows)

                -- [CALCITE-1016] "GROUP BY constant" on empty relation should return 0 rows
                -- Should return 0 rows
                -- disambiguated group by from 1 to '2'
                select '1' from emp where false group by '2';
                +--------+
                | EXPR$0 |
                +--------+
                +--------+
                (0 rows)

                -- Should return 0 rows
                -- disambiguated group by from 1 to '2'
                select count('1') from emp where false group by '2';
                +--------+
                | EXPR$0 |
                +--------+
                +--------+
                (0 rows)

                -- Should return 1 row
                select count('1') from emp where false group by ();
                +--------+
                | EXPR$0 |
                +--------+
                |      0 |
                +--------+
                (1 row)

                -- Should return 1 row
                select count('1') from emp where false;
                +--------+
                | EXPR$0 |
                +--------+
                |      0 |
                +--------+
                (1 row)

                -- As above, but on VALUES rather than table
                -- Should return 0 rows
                -- disambiguated group by from 1 to '2'
                select '1' from (values (1, 2), (3, 4)) where false group by '2';
                +--------+
                | EXPR$0 |
                +--------+
                +--------+
                (0 rows)

                -- Should return 0 rows
                -- disambiguated group by from 1 to '2'
                select count('1') from (values (1, 2), (3, 4)) where false group by '2';
                +--------+
                | EXPR$0 |
                +--------+
                +--------+
                (0 rows)

                -- Should return 1 row
                select count('1') from (values (1, 2), (3, 4)) where false group by ();
                +--------+
                | EXPR$0 |
                +--------+
                |      0 |
                +--------+
                (1 row)

                -- Should return 1 row
                select count('1') from (values (1, 2), (3, 4)) where false;
                +--------+
                | EXPR$0 |
                +--------+
                |      0 |
                +--------+
                (1 row)

                -- As above, but on join
                -- Should return 0 rows
                -- disambiguated group by from 1 to '2'
                select '1' from emp join dept using (deptno) where false group by '2';
                +--------+
                | EXPR$0 |
                +--------+
                +--------+
                (0 rows)

                -- Should return 0 rows
                -- disambiguated group by from 1 to '2'
                select count('1') from emp join dept using (deptno) where false group by '2';
                +--------+
                | EXPR$0 |
                +--------+
                +--------+
                (0 rows)

                -- Should return 1 row
                select count('1') from emp join dept using (deptno) where false group by ();
                +--------+
                | EXPR$0 |
                +--------+
                |      0 |
                +--------+
                (1 row)

                -- Should return 1 row
                select count('1') from emp join dept using (deptno) where false;
                +--------+
                | EXPR$0 |
                +--------+
                |      0 |
                +--------+
                (1 row)

                -- [CALCITE-5425] Should not pushdown Filter through Aggregate without group keys
                -- Should return 0 rows
                select count(*) from emp having false;
                +--------+
                | EXPR$0 |
                +--------+
                +--------+
                (0 rows)

                -- [CALCITE-1023] Planner rule that removes Aggregate keys that are constant
                select job, sum(sal) as sum_sal, deptno
                from emp
                where deptno = 10
                group by deptno, job;
                +-----------+---------+--------+
                | JOB       | SUM_SAL | DEPTNO |
                +-----------+---------+--------+
                | CLERK|      1300.00 |     10 |
                | MANAGER|    2450.00 |     10 |
                | PRESIDENT|  5000.00 |     10 |
                +-----------+---------+--------+
                (3 rows)

                -- Aggregate query that uses no columns throws AssertionError in
                -- RelFieldTrimmer.trimFields
                select 2 as two
                from emp
                group by ();
                +-----+
                | TWO |
                +-----+
                |   2 |
                +-----+
                (1 row)

                -- As previous, as a scalar sub-query
                select deptno,
                  (select 2 as two from emp group by ()) as two
                from emp
                group by deptno;
                +--------+-----+
                | DEPTNO | TWO |
                +--------+-----+
                |     10 |   2 |
                |     20 |   2 |
                |     30 |   2 |
                +--------+-----+
                (3 rows)

                -- As previous, grand total
                select (select 2 from emp group by ()) as two
                from emp
                group by ();
                +-----+
                | TWO |
                +-----+
                |   2 |
                +-----+
                (1 row)
                """);
    }

    @Test public void testArgMax() {
        // Results differ from the Calcite test
        // because ARG_MIN and ARG_MAX are non-deterministic
        this.qs("""
                -- ARG_MIN, ARG_MAX without GROUP BY
                select arg_min(ename, deptno) as mi, arg_max(ename, deptno) as ma
                from emp;
                +------+-----+
                | MI   | MA  |
                +------+-----+
                | CLARK| WARD|
                +------+-----+
                (1 row)

                -- ARG_MIN, ARG_MAX with DISTINCT
                select arg_min(distinct ename, deptno) as mi, arg_max(distinct ename, deptno) as ma
                from emp;
                +------+-----+
                | MI   | MA  |
                +------+-----+
                | CLARK| WARD|
                +------+-----+
                (1 row)

                -- ARG_MIN, ARG_MAX function with WHERE.
                select arg_min(ename, deptno) as mi, arg_max(ename, deptno) as ma
                from emp
                where deptno <= 20;
                +------+------+
                | MI   | MA   |
                +------+------+
                | CLARK| SMITH|
                +------+------+
                (1 row)

                -- ARG_MIN, ARG_MAX function with WHERE that removes all rows.
                -- Result is NULL even though ARG_MIN, ARG_MAX is applied to a not-NULL column.
                select arg_min(ename, deptno) as mi, arg_max(ename, deptno) as ma
                from emp
                where deptno > 60;
                +----+----+
                | MI | MA |
                +----+----+
                |NULL|NULL|
                +----+----+
                (1 row)

                -- ARG_MIN, ARG_MAX function with GROUP BY. note that key is NULL but result is not NULL.
                select deptno, arg_min(ename, ename) as mi, arg_max(ename, ename) as ma
                from emp
                group by deptno;
                +--------+-------+------+
                | DEPTNO | MI    | MA   |
                +--------+-------+------+
                |     10 | CLARK| MILLER|
                |     20 | ADAMS| SMITH|
                |     30 | ALLEN| WARD|
                +--------+-------+------+
                (3 rows)

                -- ARG_MIN, ARG_MAX applied to an integer.
                select arg_min(deptno, empno) as mi,
                  arg_max(deptno, empno) as ma,
                  arg_max(deptno, empno) filter (where job = 'MANAGER') as mamgr
                from emp;
                +----+----+-------+
                | MI | MA | MAMGR |
                +----+----+-------+
                | 20 | 10 |    10 |
                +----+----+-------+
                (1 row)

                -- DISTINCT query with ORDER BY on aggregate when there is an implicit cast
                select distinct sum(deptno + '1') as deptsum from dept order by 1;
                +---------+
                | DEPTSUM |
                +---------+
                |     104 |
                +---------+
                (1 row)""");
    }

    @Test
    public void testAggregates5() {
        this.qs("""
                   -- [CALCITE-729] IndexOutOfBoundsException in ROLLUP query on JDBC data source
                   select deptno, job, count(*) as c
                   from emp
                   group by rollup (deptno, job)
                   order by 1, 2;
                   +--------+-----------+----+
                   | DEPTNO | JOB       | C  |
                   +--------+-----------+----+
                   |     10 | CLERK|       1 |
                   |     10 | MANAGER|     1 |
                   |     10 | PRESIDENT|   1 |
                   |     10 |NULL       |  3 |
                   |     20 | ANALYST|     2 |
                   |     20 | CLERK|       2 |
                   |     20 | MANAGER|     1 |
                   |     20 |NULL       |  5 |
                   |     30 | CLERK|       1 |
                   |     30 | MANAGER|     1 |
                   |     30 | SALESMAN|    4 |
                   |     30 |NULL       |  6 |
                   |        |NULL       | 14 |
                   +--------+-----------+----+
                   (13 rows)

                   -- [CALCITE-799] Incorrect result for "HAVING count(*) > 1"
                   select d.deptno, min(e.empid) as empid
                   from (values (100, 'Bill', 1),
                                (200, 'Eric', 1),
                                (150, 'Sebastian', 3)) as e(empid, name, deptno)
                   join (values (1, 'LeaderShip'),
                                (2, 'TestGroup'),
                                (3, 'Development')) as d(deptno, name)
                   on e.deptno = d.deptno
                   group by d.deptno
                   having count(*) > 1;
                   +--------+-------+
                   | DEPTNO | EMPID |
                   +--------+-------+
                   |      1 |   100 |
                   +--------+-------+
                   (1 row)

                   -- Same, using USING (combining [CALCITE-799] and [CALCITE-801])
                   select d.deptno, min(e.empid) as empid
                   from (values (100, 'Bill', 1),
                                (200, 'Eric', 1),
                                (150, 'Sebastian', 3)) as e(empid, name, deptno)
                   join (values (1, 'LeaderShip'),
                                (2, 'TestGroup'),
                                (3, 'Development')) as d(deptno, name)
                   using (deptno)
                   group by d.deptno
                   having count(*) > 1;
                   +--------+-------+
                   | DEPTNO | EMPID |
                   +--------+-------+
                   |      1 |   100 |
                   +--------+-------+
                   (1 row)""");
    }

    @Test
    public void testAgg3() {
        this.qs("""
                -- [CALCITE-4345] SUM(CASE WHEN b THEN 1) etc.
                select
                 sum(sal) as sum_sal,
                 count(distinct case
                       when job = 'CLERK'
                       then deptno else null end) as count_distinct_clerk,
                 sum(case when deptno = 10 then sal end) as sum_sal_d10,
                 sum(case when deptno = 20 then sal else 0 end) as sum_sal_d20,
                 sum(case when deptno = 30 then 1 else 0 end) as count_d30,
                 count(case when deptno = 40 then 'x' end) as count_d40,
                 sum(case when deptno = 45 then 1 end) as count_d45,
                 sum(case when deptno = 50 then 1 else null end) as count_d50,
                 sum(case when deptno = 60 then null end) as sum_null_d60,
                 sum(case when deptno = 70 then null else 1 end) as sum_null_d70,
                 count(case when deptno = 20 then 1 end) as count_d20
                from emp;
                +----------+----------------------+-------------+-------------+-----------+-----------+-----------+-----------+--------------+--------------+-----------+
                | SUM_SAL  | COUNT_DISTINCT_CLERK | SUM_SAL_D10 | SUM_SAL_D20 | COUNT_D30 | COUNT_D40 | COUNT_D45 | COUNT_D50 | SUM_NULL_D60 | SUM_NULL_D70 | COUNT_D20 |
                +----------+----------------------+-------------+-------------+-----------+-----------+-----------+-----------+--------------+--------------+-----------+
                | 29025.00 |                    3 |     8750.00 |    10875.00 |         6 |         0 |           |           |              |           14 |         5 |
                +----------+----------------------+-------------+-------------+-----------+-----------+-----------+-----------+--------------+--------------+-----------+
                (1 row)

                -- Check that SUM produces NULL on empty set, COUNT produces 0.
                select
                 sum(sal) as sum_sal,
                 count(distinct case
                       when job = 'CLERK'
                       then deptno else null end) as count_distinct_clerk,
                 sum(case when deptno = 10 then sal end) as sum_sal_d10,
                 sum(case when deptno = 20 then sal else 0 end) as sum_sal_d20,
                 sum(case when deptno = 30 then 1 else 0 end) as count_d30,
                 count(case when deptno = 40 then 'x' end) as count_d40,
                 sum(case when deptno = 45 then 1 end) as count_d45,
                 sum(case when deptno = 50 then 1 else null end) as count_d50,
                 sum(case when deptno = 60 then null end) as sum_null_d60,
                 sum(case when deptno = 70 then null else 1 end) as sum_null_d70,
                 count(case when deptno = 20 then 1 end) as count_d20
                from emp
                where false;
                +---------+----------------------+-------------+-------------+-----------+-----------+-----------+-----------+--------------+--------------+-----------+
                | SUM_SAL | COUNT_DISTINCT_CLERK | SUM_SAL_D10 | SUM_SAL_D20 | COUNT_D30 | COUNT_D40 | COUNT_D45 | COUNT_D50 | SUM_NULL_D60 | SUM_NULL_D70 | COUNT_D20 |
                +---------+----------------------+-------------+-------------+-----------+-----------+-----------+-----------+--------------+--------------+-----------+
                |         |                    0 |             |             |           |         0 |           |           |              |              |         0 |
                +---------+----------------------+-------------+-------------+-----------+-----------+-----------+-----------+--------------+--------------+-----------+
                (1 row)

                -- [CALCITE-4609] AggregateRemoveRule throws while handling AVG
                -- Note that the outer GROUP BY is a no-op, and therefore
                -- AggregateRemoveRule kicks in.
                SELECT job, AVG(avg_sal) AS avg_sal2
                FROM (
                    SELECT deptno, job, AVG(sal) AS avg_sal
                    FROM emp
                    GROUP BY deptno, job) AS EmpAnalytics
                WHERE deptno = 30
                GROUP BY job;
                +----------+----------+
                | JOB      | AVG_SAL2 |
                +----------+----------+
                | CLERK|   950.00     |
                | MANAGER|  2850.00   |
                | SALESMAN|  1400.00  |
                +----------+----------+
                (3 rows)

                -- Same, using WITH
                WITH EmpAnalytics AS (
                    SELECT deptno, job, AVG(sal) AS avg_sal
                    FROM emp
                    GROUP BY deptno, job)
                SELECT job, AVG(avg_sal) AS avg_sal2
                FROM EmpAnalytics
                WHERE deptno = 30
                GROUP BY job;
                +----------+----------+
                | JOB      | AVG_SAL2 |
                +----------+----------+
                | CLERK|       950.00 |
                | MANAGER|    2850.00 |
                | SALESMAN|   1400.00 |
                +----------+----------+
                (3 rows)""");
    }

    @Test
    public void testAgg4() {
        this.qs("""
                -- [CALCITE-1930] AggregateExpandDistinctAggregateRules should handle multiple aggregate calls with same input ref
                select count(distinct EMPNO), COUNT(SAL), MIN(SAL), MAX(SAL) from emp;
                +--------+--------+--------+---------+
                | EXPR$0 | EXPR$1 | EXPR$2 | EXPR$3  |
                +--------+--------+--------+---------+
                |     14 |     14 | 800.00 | 5000.00 |
                +--------+--------+--------+---------+
                (1 row)

                -- [CALCITE-1930] AggregateExpandDistinctAggregateRules should handle multiple aggregate calls with same input ref
                select count(distinct DEPTNO), COUNT(JOB), MIN(SAL), MAX(SAL) from emp;
                +--------+--------+--------+---------+
                | EXPR$0 | EXPR$1 | EXPR$2 | EXPR$3  |
                +--------+--------+--------+---------+
                |      3 |     14 | 800.00 | 5000.00 |
                +--------+--------+--------+---------+
                (1 row)

                -- [CALCITE-1930] AggregateExpandDistinctAggregateRules should handle multiple aggregate calls with same input ref
                select MGR, count(distinct DEPTNO), COUNT(JOB), MIN(SAL), MAX(SAL) from emp group by MGR;
                +------+--------+--------+---------+---------+
                | MGR  | EXPR$1 | EXPR$2 | EXPR$3  | EXPR$4  |
                +------+--------+--------+---------+---------+
                | 7566 |      1 |      2 | 3000.00 | 3000.00 |
                | 7698 |      1 |      5 |  950.00 | 1600.00 |
                | 7782 |      1 |      1 | 1300.00 | 1300.00 |
                | 7788 |      1 |      1 | 1100.00 | 1100.00 |
                | 7839 |      3 |      3 | 2450.00 | 2975.00 |
                | 7902 |      1 |      1 |  800.00 |  800.00 |
                |      |      1 |      1 | 5000.00 | 5000.00 |
                +------+--------+--------+---------+---------+
                (7 rows)

                -- [CALCITE-1930] AggregateExpandDistinctAggregateRules should handle multiple aggregate calls with same input ref
                select MGR, count(distinct DEPTNO, JOB), MIN(SAL), MAX(SAL) from emp group by MGR;
                +------+--------+---------+---------+
                | MGR  | EXPR$1 | EXPR$2  | EXPR$3  |
                +------+--------+---------+---------+
                | 7566 |      1 | 3000.00 | 3000.00 |
                | 7698 |      2 |  950.00 | 1600.00 |
                | 7782 |      1 | 1300.00 | 1300.00 |
                | 7788 |      1 | 1100.00 | 1100.00 |
                | 7839 |      3 | 2450.00 | 2975.00 |
                | 7902 |      1 |  800.00 |  800.00 |
                |      |      1 | 5000.00 | 5000.00 |
                +------+--------+---------+---------+
                (7 rows)""");
    }

    @Test @Ignore("Several not-implemented aggregation functions")
    public void testRegrValue() {
        this.qs("""
                -- [CALCITE-1776, CALCITE-2402] REGR_COUNT
                SELECT regr_count(COMM, SAL) as "REGR_COUNT(COMM, SAL)",
                   regr_count(EMPNO, SAL) as "REGR_COUNT(EMPNO, SAL)"
                from emp;
                +-----------------------+------------------------+
                | REGR_COUNT(COMM, SAL) | REGR_COUNT(EMPNO, SAL) |
                +-----------------------+------------------------+
                |                     4 |                     14 |
                +-----------------------+------------------------+
                (1 row)

                -- [CALCITE-1776, CALCITE-2402] REGR_SXX, REGR_SXY, REGR_SYY
                SELECT
                  regr_sxx(COMM, SAL) as "REGR_SXX(COMM, SAL)",
                  regr_syy(COMM, SAL) as "REGR_SYY(COMM, SAL)",
                  regr_sxx(SAL, COMM) as "REGR_SXX(SAL, COMM)",
                  regr_syy(SAL, COMM) as "REGR_SYY(SAL, COMM)"
                from emp;
                +---------------------+---------------------+---------------------+---------------------+
                | REGR_SXX(COMM, SAL) | REGR_SYY(COMM, SAL) | REGR_SXX(SAL, COMM) | REGR_SYY(SAL, COMM) |
                +---------------------+---------------------+---------------------+---------------------+
                |          95000.0000 |        1090000.0000 |        1090000.0000 |          95000.0000 |
                +---------------------+---------------------+---------------------+---------------------+
                (1 row)

                -- [CALCITE-1776, CALCITE-2402] COVAR_POP, COVAR_SAMP, VAR_SAMP, VAR_POP
                SELECT
                  covar_pop(COMM, COMM) as "COVAR_POP(COMM, COMM)",
                  covar_samp(SAL, SAL) as "COVAR_SAMP(SAL, SAL)",
                  var_pop(COMM) as "VAR_POP(COMM)",
                  var_samp(SAL) as "VAR_SAMP(SAL)"
                from emp;
                +-----------------------+----------------------+---------------+-------------------+
                | COVAR_POP(COMM, COMM) | COVAR_SAMP(SAL, SAL) | VAR_POP(COMM) | VAR_SAMP(SAL)     |
                +-----------------------+----------------------+---------------+-------------------+
                |           272500.0000 |    1398313.873626374 |   272500.0000 | 1398313.873626374 |
                +-----------------------+----------------------+---------------+-------------------+
                (1 row)""");
    }

    @Test
    public void bitTests() {
        this.qs("""
                -- BIT_AND, BIT_OR, BIT_XOR aggregate functions
                select bit_and(deptno), bit_or(deptno), bit_xor(deptno) from emp;
                +--------+--------+--------+
                | EXPR$0 | EXPR$1 | EXPR$2 |
                +--------+--------+--------+
                |      0 |     30 |     30 |
                +--------+--------+--------+
                (1 row)

                select deptno, bit_and(empno), bit_or(empno), bit_xor(empno) from emp group by deptno;
                +--------+--------+--------+--------+
                | DEPTNO | EXPR$1 | EXPR$2 | EXPR$3 |
                +--------+--------+--------+--------+
                |     10 |   7686 |   7935 |   7687 |
                |     20 |   7168 |   8191 |   7985 |
                |     30 |   7168 |   8191 |    934 |
                +--------+--------+--------+--------+
                (3 rows)""");
    }
}
