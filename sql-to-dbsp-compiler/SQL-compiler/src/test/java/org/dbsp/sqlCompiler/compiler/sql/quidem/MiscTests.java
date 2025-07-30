package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Ignore;
import org.junit.Test;

/** Tests from Calcite misc.iq */
public class MiscTests extends ScottBaseTests {
    @Test
    public void uuidTests() {
        this.qs("""
                SELECT UUID '123e4567-e89b-12d3-a456-426655440000';
                +--------------------------------------+
                | EXPR$0                               |
                +--------------------------------------+
                | 123e4567-e89b-12d3-a456-426655440000 |
                +--------------------------------------+
                (1 row)
                
                SELECT CAST('123e4567-e89b-12d3-a456-426655440000' AS UUID);
                +--------------------------------------+
                | EXPR$0                               |
                +--------------------------------------+
                | 123e4567-e89b-12d3-a456-426655440000 |
                +--------------------------------------+
                (1 row)
                
                SELECT CAST(UUID '123e4567-e89b-12d3-a456-426655440000' AS VARCHAR);
                +-------------------------------------+
                | EXPR$0                              |
                +-------------------------------------+
                | 123e4567-e89b-12d3-a456-426655440000|
                +-------------------------------------+
                (1 row)
                
                SELECT CAST(UUID '123e4567-e89b-12d3-a456-426655440000' AS VARBINARY);
                +----------------------------------+
                | EXPR$0                           |
                +----------------------------------+
                | 123e4567e89b12d3a456426655440000 |
                +----------------------------------+
                (1 row)
                
                SELECT CAST(x'123e4567e89b12d3a456426655440000' AS UUID);
                +--------------------------------------+
                | EXPR$0                               |
                +--------------------------------------+
                | 123e4567-e89b-12d3-a456-426655440000 |
                +--------------------------------------+
                (1 row)
                
                SELECT UUID '123e4567-e89b-12d3-a456-426655440000' = '123e4567-e89b-12d3-a456-426655440000';
                +--------+
                | EXPR$0 |
                +--------+
                | true   |
                +--------+
                (1 row)
                
                SELECT CAST(NULL AS UUID);
                +--------+
                | EXPR$0 |
                +--------+
                |        |
                +--------+
                (1 row)""");
        this.qf("SELECT CAST('123e' AS UUID)",
                "Cannot parse", false);
        this.qf("SELECT CAST(x'00' AS UUID)",
                "Need at least 16 bytes", false);
        this.queryFailingInCompilation("SELECT UUID NULL", "Incorrect syntax");
    }

    @Test
    public void rowTests() {
        this.qs("""
                -- Implicit ROW
                select deptno, (empno, deptno) as r
                from emp;
                +--------+------------+
                | DEPTNO | R          |
                +--------+------------+
                |     10 | {7782, 10} |
                |     10 | {7839, 10} |
                |     10 | {7934, 10} |
                |     20 | {7369, 20} |
                |     20 | {7566, 20} |
                |     20 | {7788, 20} |
                |     20 | {7876, 20} |
                |     20 | {7902, 20} |
                |     30 | {7499, 30} |
                |     30 | {7521, 30} |
                |     30 | {7654, 30} |
                |     30 | {7698, 30} |
                |     30 | {7844, 30} |
                |     30 | {7900, 30} |
                +--------+------------+
                (14 rows)
                
                -- Explicit ROW
                select deptno, row (empno, deptno) as r
                from emp;
                +--------+------------+
                | DEPTNO | R          |
                +--------+------------+
                |     10 | {7782, 10} |
                |     10 | {7839, 10} |
                |     10 | {7934, 10} |
                |     20 | {7369, 20} |
                |     20 | {7566, 20} |
                |     20 | {7788, 20} |
                |     20 | {7876, 20} |
                |     20 | {7902, 20} |
                |     30 | {7499, 30} |
                |     30 | {7521, 30} |
                |     30 | {7654, 30} |
                |     30 | {7698, 30} |
                |     30 | {7844, 30} |
                |     30 | {7900, 30} |
                +--------+------------+
                (14 rows)
                
                -- [CALCITE-5960] CAST failed if SqlTypeFamily of targetType is NULL
                -- Cast row
                SELECT cast(row(1, 2) as row(a integer, b tinyint)) as r;
                +--------+
                | R      |
                +--------+
                | {1, 2} |
                +--------+
                (1 row)""");
    }

    @Test
    @Ignore("Requires MULTISET")
    public void testRowCoalesce() {
        this.qs("""
                -- [CALCITE-877] Allow ROW as argument to COLLECT
                select deptno, collect(r) as empnos
                from (select deptno, (empno, deptno) as r
                  from emp)
                group by deptno;
                +--------+--------------------------------------------------------------------------+
                | DEPTNO | EMPNOS                                                                   |
                +--------+--------------------------------------------------------------------------+
                |     10 | [{7782, 10}, {7839, 10}, {7934, 10}]                                     |
                |     20 | [{7369, 20}, {7566, 20}, {7788, 20}, {7876, 20}, {7902, 20}]             |
                |     30 | [{7499, 30}, {7521, 30}, {7654, 30}, {7698, 30}, {7844, 30}, {7900, 30}] |
                +--------+--------------------------------------------------------------------------+
                (3 rows)""");
    }

    @Test
    public void intervalTests() {
        // Added tests with decimal and FP
        this.qs("""
                -- [CALCITE-922] Value of INTERVAL literal
                select deptno * interval '2' day as d2,
                 deptno * interval -'3' hour as h3,
                 deptno * interval -'-4' hour as h4,
                 deptno * interval -'4:30' hour to minute as h4_5,
                 deptno * interval -'-1-3' year to month as y1_25,
                 CAST(deptno AS DECIMAL(6, 2)) / 10 * interval 1 minutes as m,
                 CAST(deptno AS REAL) / 15 * interval 1 day as d
                from dept;
                +---------+------------+-----------+------------+--------------------+--------+-----------------+
                | D2      | H3         | H4        | H4_5       | Y1_25              | M      | D               |
                +---------+------------+-----------+------------+--------------------+--------+-----------------+
                | 20 days |  -30 hours |  40 hours |  -45 hours | 12 years 06 months | 1 mins |  57600.001 secs |
                | 40 days |  -60 hours |  80 hours |  -90 hours | 25 years 00 months | 2 mins | 115200.003 secs |
                | 60 days |  -90 hours | 120 hours | -135 hours | 37 years 06 months | 3 mins | 172800 secs     |
                | 80 days | -120 hours | 160 hours | -180 hours | 50 years 00 months | 4 mins | 230400.006 secs |
                +---------+------------+-----------+------------+--------------------+--------+-----------------+
                (4 rows)
                
                -- [CALCITE-4091] Interval expressions
                select empno, mgr, date '1970-01-01' + interval empno day as d,
                  timestamp '1970-01-01 00:00:00' + interval (mgr / 100) minute as ts
                from emp
                order by empno;
                +-------+------+------------+---------------------+
                | EMPNO | MGR  | D          | TS                  |
                +-------+------+------------+---------------------+
                |  7369 | 7902 | 1990-03-06 | 1970-01-01 01:19:00 |
                |  7499 | 7698 | 1990-07-14 | 1970-01-01 01:16:00 |
                |  7521 | 7698 | 1990-08-05 | 1970-01-01 01:16:00 |
                |  7566 | 7839 | 1990-09-19 | 1970-01-01 01:18:00 |
                |  7654 | 7698 | 1990-12-16 | 1970-01-01 01:16:00 |
                |  7698 | 7839 | 1991-01-29 | 1970-01-01 01:18:00 |
                |  7782 | 7839 | 1991-04-23 | 1970-01-01 01:18:00 |
                |  7788 | 7566 | 1991-04-29 | 1970-01-01 01:15:00 |
                |  7839 |      | 1991-06-19 |                     |
                |  7844 | 7698 | 1991-06-24 | 1970-01-01 01:16:00 |
                |  7876 | 7788 | 1991-07-26 | 1970-01-01 01:17:00 |
                |  7900 | 7698 | 1991-08-19 | 1970-01-01 01:16:00 |
                |  7902 | 7566 | 1991-08-21 | 1970-01-01 01:15:00 |
                |  7934 | 7782 | 1991-09-22 | 1970-01-01 01:17:00 |
                +-------+------+------------+---------------------+
                (14 rows)
                
                -- [CALCITE-6581] INTERVAL with WEEK and QUARTER
                select timestamp '1970-01-01' + interval '2' week as w,
                  timestamp '1970-01-01 00:00:00' + interval '2' quarter as q;
                +---------------------+---------------------+
                | W                   | Q                   |
                +---------------------+---------------------+
                | 1970-01-15 00:00:00 | 1970-07-01 00:00:00 |
                +---------------------+---------------------+
                (1 row)
                """);
    }

    @Test
    public void intervalDivision() {
        // Tested on Postgres, the long interval results differ, since postgres computes on days
        this.qs("""
                select interval '2' day / deptno as d2,
                 interval -'3' hour / deptno as h3,
                 interval -'-4' hour / deptno as h4,
                 interval -'4:30' hour to minute / deptno as h4_5,
                 interval -'-1-3' year to month / deptno as y1_25,
                 interval 1 minutes / CAST(deptno AS DECIMAL(6, 2)) as m
                from dept;
                +-----------------+-----------------+---------+-------------------+----------+----------+
                | D2              | H3              | H4      | H4_5              | Y1_25    | M        |
                +-----------------+-----------------+---------+-------------------+----------+----------+
                | 4 hours 48 mins |        -18 mins | 24 mins |          -27 mins | 1 months |   6 secs |
                | 2 hours 24 mins |         -9 mins | 12 mins |  -13 mins 30 secs | 0 months |   3 secs |
                | 1 hour 36 mins  |         -6 mins |  8 mins |           -9 mins | 0 months |   2 secs |
                | 1 hour 12 mins  | -4 mins 30 secs |  6 mins |   -6 mins 45 secs | 0 months | 1.5 secs |
                +-----------------+-----------------+---------+-------------------+----------+----------+
                (4 rows)""");
    }
}