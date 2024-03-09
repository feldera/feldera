package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Ignore;
import org.junit.Test;

// AggTests that use the Scott database
// https://github.com/apache/calcite/blob/main/core/src/test/resources/sql/agg.iq
public class AggScottTests extends ScottBaseTests {
    @Ignore("Grouping not yet implemented")
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
                |     10 | CLERK     |  1 | 0 | 0 | 0 |
                |     10 | MANAGER   |  1 | 0 | 0 | 0 |
                |     10 | PRESIDENT |  1 | 0 | 0 | 0 |
                |     10 |           |  3 | 0 | 1 | 1 |
                |     20 | ANALYST   |  2 | 0 | 0 | 0 |
                |     20 | CLERK     |  2 | 0 | 0 | 0 |
                |     20 | MANAGER   |  1 | 0 | 0 | 0 |
                |     20 |           |  5 | 0 | 1 | 1 |
                |     30 | CLERK     |  1 | 0 | 0 | 0 |
                |     30 | MANAGER   |  1 | 0 | 0 | 0 |
                |     30 | SALESMAN  |  4 | 0 | 0 | 0 |
                |     30 |           |  6 | 0 | 1 | 1 |
                |        | ANALYST   |  2 | 1 | 0 | 2 |
                |        | CLERK     |  4 | 1 | 0 | 2 |
                |        | MANAGER   |  3 | 1 | 0 | 2 |
                |        | PRESIDENT |  1 | 1 | 0 | 2 |
                |        | SALESMAN  |  4 | 1 | 0 | 2 |
                |        |           | 14 | 1 | 1 | 3 |
                +--------+-----------+----+---+---+---+
                (18 rows)""");
    }

    @Test @Ignore("Grouping not yet implemented")
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
                  |     10 | CLERK     |  7934 | MILLER |  1300.00 | grouped by deptno,job,empno,ename |
                  |     10 | CLERK     |       |        |  1300.00 | grouped by deptno,job             |
                  |     10 | MANAGER   |  7782 | CLARK  |  2450.00 | grouped by deptno,job,empno,ename |
                  |     10 | MANAGER   |       |        |  2450.00 | grouped by deptno,job             |
                  |     10 | PRESIDENT |  7839 | KING   |  5000.00 | grouped by deptno,job,empno,ename |
                  |     10 | PRESIDENT |       |        |  5000.00 | grouped by deptno,job             |
                  |     10 |           |       |        |  8750.00 | grouped by deptno                 |
                  |     20 | ANALYST   |  7788 | SCOTT  |  3000.00 | grouped by deptno,job,empno,ename |
                  |     20 | ANALYST   |  7902 | FORD   |  3000.00 | grouped by deptno,job,empno,ename |
                  |     20 | ANALYST   |       |        |  6000.00 | grouped by deptno,job             |
                  |     20 | CLERK     |  7369 | SMITH  |   800.00 | grouped by deptno,job,empno,ename |
                  |     20 | CLERK     |  7876 | ADAMS  |  1100.00 | grouped by deptno,job,empno,ename |
                  |     20 | CLERK     |       |        |  1900.00 | grouped by deptno,job             |
                  |     20 | MANAGER   |  7566 | JONES  |  2975.00 | grouped by deptno,job,empno,ename |
                  |     20 | MANAGER   |       |        |  2975.00 | grouped by deptno,job             |
                  |     20 |           |       |        | 10875.00 | grouped by deptno                 |
                  |     30 | CLERK     |  7900 | JAMES  |   950.00 | grouped by deptno,job,empno,ename |
                  |     30 | CLERK     |       |        |   950.00 | grouped by deptno,job             |
                  |     30 | MANAGER   |  7698 | BLAKE  |  2850.00 | grouped by deptno,job,empno,ename |
                  |     30 | MANAGER   |       |        |  2850.00 | grouped by deptno,job             |
                  |     30 | SALESMAN  |  7499 | ALLEN  |  1600.00 | grouped by deptno,job,empno,ename |
                  |     30 | SALESMAN  |  7521 | WARD   |  1250.00 | grouped by deptno,job,empno,ename |
                  |     30 | SALESMAN  |  7654 | MARTIN |  1250.00 | grouped by deptno,job,empno,ename |
                  |     30 | SALESMAN  |  7844 | TURNER |  1500.00 | grouped by deptno,job,empno,ename |
                  |     30 | SALESMAN  |       |        |  5600.00 | grouped by deptno,job             |
                  |     30 |           |       |        |  9400.00 | grouped by deptno                 |
                  |        |           |       |        | 29025.00 | grouped by ()                     |
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
                  |     10 | CLERK     |  7934 | MILLER |  1300.00 | grouped by deptno,job,empno,ename |
                  |     10 | CLERK     |       |        |  1300.00 | grouped by deptno,job             |
                  |     10 | MANAGER   |  7782 | CLARK  |  2450.00 | grouped by deptno,job,empno,ename |
                  |     10 | MANAGER   |       |        |  2450.00 | grouped by deptno,job             |
                  |     10 | PRESIDENT |  7839 | KING   |  5000.00 | grouped by deptno,job,empno,ename |
                  |     10 | PRESIDENT |       |        |  5000.00 | grouped by deptno,job             |
                  |     10 |           |       |        |  8750.00 | grouped by deptno, grouping set 3 |
                  |     10 |           |       |        |  8750.00 | grouped by deptno, grouping set 4 |
                  |     20 | ANALYST   |  7788 | SCOTT  |  3000.00 | grouped by deptno,job,empno,ename |
                  |     20 | ANALYST   |  7902 | FORD   |  3000.00 | grouped by deptno,job,empno,ename |
                  |     20 | ANALYST   |       |        |  6000.00 | grouped by deptno,job             |
                  |     20 | CLERK     |  7369 | SMITH  |   800.00 | grouped by deptno,job,empno,ename |
                  |     20 | CLERK     |  7876 | ADAMS  |  1100.00 | grouped by deptno,job,empno,ename |
                  |     20 | CLERK     |       |        |  1900.00 | grouped by deptno,job             |
                  |     20 | MANAGER   |  7566 | JONES  |  2975.00 | grouped by deptno,job,empno,ename |
                  |     20 | MANAGER   |       |        |  2975.00 | grouped by deptno,job             |
                  |     20 |           |       |        | 10875.00 | grouped by deptno, grouping set 3 |
                  |     20 |           |       |        | 10875.00 | grouped by deptno, grouping set 4 |
                  |     30 | CLERK     |  7900 | JAMES  |   950.00 | grouped by deptno,job,empno,ename |
                  |     30 | CLERK     |       |        |   950.00 | grouped by deptno,job             |
                  |     30 | MANAGER   |  7698 | BLAKE  |  2850.00 | grouped by deptno,job,empno,ename |
                  |     30 | MANAGER   |       |        |  2850.00 | grouped by deptno,job             |
                  |     30 | SALESMAN  |  7499 | ALLEN  |  1600.00 | grouped by deptno,job,empno,ename |
                  |     30 | SALESMAN  |  7521 | WARD   |  1250.00 | grouped by deptno,job,empno,ename |
                  |     30 | SALESMAN  |  7654 | MARTIN |  1250.00 | grouped by deptno,job,empno,ename |
                  |     30 | SALESMAN  |  7844 | TURNER |  1500.00 | grouped by deptno,job,empno,ename |
                  |     30 | SALESMAN  |       |        |  5600.00 | grouped by deptno,job             |
                  |     30 |           |       |        |  9400.00 | grouped by deptno, grouping set 3 |
                  |     30 |           |       |        |  9400.00 | grouped by deptno, grouping set 4 |
                  |        |           |       |        | 29025.00 | grouped by (), grouping set 5     |
                  |        |           |       |        | 29025.00 | grouped by (), grouping set 6     |
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
                  (28 rows)
                                  
                  -- Equivalent query, but with GROUP_ID and GROUPING_ID
                  select sum(sal) as s,
                    grouping_id(job, deptno, comm is null) as g,
                    group_id() as i
                  from emp
                  group by grouping sets ((job, deptno, comm is null),
                    (job, deptno), (job, comm is null), (job, comm is null))
                  order by g, i, s desc;
                  +---------+---+---+
                  | S       | G | I |
                  +---------+---+---+
                  | 6000.00 | 0 | 0 |
                  | 5600.00 | 0 | 0 |
                  | 5000.00 | 0 | 0 |
                  | 2975.00 | 0 | 0 |
                  | 2850.00 | 0 | 0 |
                  | 2450.00 | 0 | 0 |
                  | 1900.00 | 0 | 0 |
                  | 1300.00 | 0 | 0 |
                  |  950.00 | 0 | 0 |
                  | 8275.00 | 0 | 1 |
                  | 6000.00 | 0 | 1 |
                  | 5600.00 | 0 | 1 |
                  | 5000.00 | 0 | 1 |
                  | 4150.00 | 0 | 1 |
                  | 6000.00 | 1 | 0 |
                  | 5600.00 | 1 | 0 |
                  | 5000.00 | 1 | 0 |
                  | 2975.00 | 1 | 0 |
                  | 2850.00 | 1 | 0 |
                  | 2450.00 | 1 | 0 |
                  | 1900.00 | 1 | 0 |
                  | 1300.00 | 1 | 0 |
                  |  950.00 | 1 | 0 |
                  | 8275.00 | 2 | 0 |
                  | 6000.00 | 2 | 0 |
                  | 5600.00 | 2 | 0 |
                  | 5000.00 | 2 | 0 |
                  | 4150.00 | 2 | 0 |
                  +---------+---+---+
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

    @Test @Ignore("https://github.com/feldera/feldera/issues/1508")
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

    @Test @Ignore("https://github.com/feldera/feldera/issues/1481")
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
                -- [CALCITE-1293] Bad code generated when argument to COUNT(DISTINCT) is a
                -- GROUP BY column
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

    @Test @Ignore("https://github.com/feldera/feldera/issues/1506")
    public void testDistinctCount() {
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
                (4 rows)

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
                (3 rows)""");
    }

    @Test @Ignore("https://github.com/feldera/feldera/issues/1506")
    public void moreTestDistinctCount() {
        this.qs("""                                
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

    @Test @Ignore("https://github.com/feldera/feldera/issues/1507")
    public void testAvg() {
        this.qs("""
                -- [CALCITE-280] BigDecimal underflow
                -- Previously threw "java.lang.ArithmeticException: Non-terminating decimal
                -- expansion; no exact representable decimal result"
                select avg(comm) as a, count(comm) as c from --
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
}
