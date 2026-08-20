package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Ignore;
import org.junit.Test;

/** Tests from Calcite misc.iq */
public class MiscTests extends ScottBaseTests {
    @Test
    public void uuidTests() {
        this.qst("""
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
                | NULL   |
                +--------+
                (1 row)

                -- A literal accepts the same spellings as a cast from a string
                SELECT UUID '123e4567e89b12d3a456426655440000';
                +--------------------------------------+
                | EXPR$0                               |
                +--------------------------------------+
                | 123e4567-e89b-12d3-a456-426655440000 |
                +--------------------------------------+
                (1 row)

                -- Hyphens are optional separators, so this denotes the same UUID.
                -- PostgreSQL accepts the same set of spellings.
                SELECT CAST('123e4567e89b12d3a456426655440000' AS UUID);
                +--------------------------------------+
                | EXPR$0                               |
                +--------------------------------------+
                | 123e4567-e89b-12d3-a456-426655440000 |
                +--------------------------------------+
                (1 row)

                SELECT CAST('{123e4567-e89b-12d3-a456-426655440000}' AS UUID);
                +--------------------------------------+
                | EXPR$0                               |
                +--------------------------------------+
                | 123e4567-e89b-12d3-a456-426655440000 |
                +--------------------------------------+
                (1 row)

                SELECT CAST('123e-4567-e89b-12d3-a456-4266-5544-0000' AS UUID);
                +--------------------------------------+
                | EXPR$0                               |
                +--------------------------------------+
                | 123e4567-e89b-12d3-a456-426655440000 |
                +--------------------------------------+
                (1 row)

                SELECT CAST('123e4567-e89b12d3-a4564266-55440000' AS UUID);
                +--------------------------------------+
                | EXPR$0                               |
                +--------------------------------------+
                | 123e4567-e89b-12d3-a456-426655440000 |
                +--------------------------------------+
                (1 row)""");
        this.qf("SELECT CAST('123e' AS UUID)",
                "Invalid UUID string '123e'", false);
        // A group is not four digits wide
        this.qf("SELECT CAST('1-2-3-4-5' AS UUID)",
                "Invalid UUID string '1-2-3-4-5'", false);
        // As above, though 36 characters long
        this.qf("SELECT CAST('123e456-7e89b-12d3-a456-426655440000' AS UUID)",
                "Invalid UUID string '123e456-7e89b-12d3-a456-426655440000'", false);
        // Empty group
        this.qf("SELECT CAST('123e4567--e89b-12d3-a456-426655440000' AS UUID)",
                "Invalid UUID string '123e4567--e89b-12d3-a456-426655440000'", false);
        // Unbalanced brace
        this.qf("SELECT CAST('{123e4567-e89b-12d3-a456-426655440000' AS UUID)",
                "Invalid UUID string '{123e4567-e89b-12d3-a456-426655440000'", false);
        // The URN form is not accepted; PostgreSQL rejects it too
        this.qf("SELECT CAST('urn:uuid:123e4567-e89b-12d3-a456-426655440000' AS UUID)",
                "Invalid UUID string 'urn:uuid:123e4567-e89b-12d3-a456-426655440000'", false);
        // Blanks are never trimmed
        this.qf("SELECT CAST('' AS UUID)", "Invalid UUID string ''", false);
        this.qf("SELECT CAST('   ' AS UUID)", "Invalid UUID string '   '", false);
        this.qf("SELECT CAST(' 123e4567-e89b-12d3-a456-426655440000' AS UUID)",
                "Invalid UUID string ' 123e4567-e89b-12d3-a456-426655440000'", false);
        this.qf("SELECT CAST(x'00' AS UUID)",
                "Need exactly 16 bytes", false);
        this.qf("SELECT CAST(x'123e4567e89b12d3a456426655440000ff' AS UUID)",
                "Need exactly 16 bytes", false);
        this.queryFailingInCompilation("SELECT UUID NULL", "Incorrect syntax");
    }

    /** Comparing a UUID with a string converts the string to a UUID;
     * a string that is not a UUID is a runtime error.
     * Tests from [CALCITE-7727], reported as issue 6883. */
    @Test
    public void issue6883() {
        this.qst("""
                SELECT UUID '123e4567-e89b-12d3-a456-426655440000'
                     = '123E4567-E89B-12D3-A456-426655440000' AS C;
                +------+
                | C    |
                +------+
                | true |
                +------+
                (1 row)

                -- Hyphens are optional, so this string denotes the same UUID
                SELECT UUID '123e4567-e89b-12d3-a456-426655440000'
                     = '123e4567e89b12d3a456426655440000' AS C;
                +------+
                | C    |
                +------+
                | true |
                +------+
                (1 row)

                -- CHAR(36) is exactly the width of the UUID, so there is no padding
                SELECT UUID '123e4567-e89b-12d3-a456-426655440000'
                     = CAST('123e4567-e89b-12d3-a456-426655440000' AS CHAR(36)) AS C;
                +------+
                | C    |
                +------+
                | true |
                +------+
                (1 row)

                -- IN uses the comparison common type
                SELECT UUID '123e4567-e89b-12d3-a456-426655440000'
                     IN ('123e4567-e89b-12d3-a456-426655440000',
                         '123E4567-E89B-12D3-A456-426655440001') AS C;
                +------+
                | C    |
                +------+
                | true |
                +------+
                (1 row)

                -- Binary compared to UUID
                SELECT UUID '123e4567-e89b-12d3-a456-426655440000'
                     = x'123e4567e89b12d3a456426655440000' AS C;
                +------+
                | C    |
                +------+
                | true |
                +------+
                (1 row)

                SELECT x'123e4567e89b12d3a456426655440000'
                     = UUID '123e4567-e89b-12d3-a456-426655440000' AS C;
                +------+
                | C    |
                +------+
                | true |
                +------+
                (1 row)

                -- A comparison with NULL is NULL, not an error
                SELECT CAST(NULL AS UUID) <> '' AS C;
                +------+
                | C    |
                +------+
                | NULL |
                +------+
                (1 row)

                SELECT u = f AS EQ_FULL, u = g AS EQ_UPPER, u = b AS EQ_BINARY
                FROM (VALUES (CAST('123e4567-e89b-12d3-a456-426655440000' AS UUID),
                              '123e4567-e89b-12d3-a456-426655440000',
                              '123E4567-E89B-12D3-A456-426655440000',
                              x'123e4567e89b12d3a456426655440000'),
                             (CAST(NULL AS UUID),
                              '123e4567-e89b-12d3-a456-426655440000',
                              '123E4567-E89B-12D3-A456-426655440000',
                              x'123e4567e89b12d3a456426655440000'))
                  AS t(u, f, g, b);
                +---------+----------+-----------+
                | EQ_FULL | EQ_UPPER | EQ_BINARY |
                +---------+----------+-----------+
                | true    | true     | true      |
                | NULL    | NULL     | NULL      |
                +---------+----------+-----------+
                (2 rows)

                SELECT CAST(u AS VARCHAR) AS C
                FROM (VALUES (CAST('123e4567-e89b-12d3-a456-426655440000' AS UUID)),
                             (CAST(NULL AS UUID))) AS t(u);
                +--------------------------------------+
                | C                                    |
                +--------------------------------------+
                | 123e4567-e89b-12d3-a456-426655440000 |
                |NULL                                  |
                +--------------------------------------+
                (2 rows)

                SELECT CAST(u AS VARBINARY) AS B, u = u AS SELF
                FROM (VALUES (CAST('123e4567-e89b-12d3-a456-426655440000' AS UUID)),
                             (CAST(NULL AS UUID))) AS t(u);
                +----------------------------------+------+
                | B                                | SELF |
                +----------------------------------+------+
                | 123e4567e89b12d3a456426655440000 | true |
                |NULL                              | NULL |
                +----------------------------------+------+
                (2 rows)

                -- UUID columns as grouping and sorting keys
                SELECT u, COUNT(*) AS C
                FROM (VALUES (CAST('123e4567-e89b-12d3-a456-426655440000' AS UUID)),
                             (CAST('123e4567-e89b-12d3-a456-426655440001' AS UUID)),
                             (CAST('123e4567-e89b-12d3-a456-426655440000' AS UUID)),
                             (CAST(NULL AS UUID))) AS t(u)
                GROUP BY u ORDER BY u;
                +--------------------------------------+---+
                | U                                    | C |
                +--------------------------------------+---+
                | 123e4567-e89b-12d3-a456-426655440000 | 2 |
                | 123e4567-e89b-12d3-a456-426655440001 | 1 |
                | NULL                                 | 1 |
                +--------------------------------------+---+
                (3 rows)

                -- UUID columns as join keys
                SELECT t1.u AS U
                FROM (VALUES (CAST('123e4567-e89b-12d3-a456-426655440000' AS UUID)),
                             (CAST('123e4567-e89b-12d3-a456-426655440001' AS UUID))) AS t1(u)
                JOIN (VALUES (CAST('123e4567-e89b-12d3-a456-426655440000' AS UUID)),
                             (CAST('123e4567-e89b-12d3-a456-426655440001' AS UUID))) AS t2(u)
                ON t1.u = t2.u
                ORDER BY 1;
                +--------------------------------------+
                | U                                    |
                +--------------------------------------+
                | 123e4567-e89b-12d3-a456-426655440000 |
                | 123e4567-e89b-12d3-a456-426655440001 |
                +--------------------------------------+
                (2 rows)""");
        // The empty string is not a UUID; before the fix this comparison returned FALSE
        this.qf("SELECT UUID '123e4567-e89b-12d3-a456-426655440000' <> ''",
                "Invalid UUID string ''", false);
        this.qf("SELECT UUID '123e4567-e89b-12d3-a456-426655440000' = '123e4567'",
                "Invalid UUID string '123e4567'", false);
        // A trailing blank does not denote a UUID either
        this.qf("SELECT UUID '123e4567-e89b-12d3-a456-426655440000' = '123e4567-e89b-12d3-a456-426655440000 '",
                "Invalid UUID string '123e4567-e89b-12d3-a456-426655440000 '", false);
        // The CHAR(40) string is padded with blanks, so converting it to a UUID fails
        this.qf("SELECT UUID '123e4567-e89b-12d3-a456-426655440000' = " +
                        "CAST('123e4567-e89b-12d3-a456-426655440000' AS CHAR(40))",
                "Invalid UUID string '123e4567-e89b-12d3-a456-426655440000    '", false);
        this.qf("SELECT UUID '123e4567-e89b-12d3-a456-426655440000' = x'00'",
                "Need exactly 16 bytes", false);
        this.qf("SELECT UUID '123e4567-e89b-12d3-a456-426655440000' = x'123e4567e89b12d3a456426655440000ff'",
                "Need exactly 16 bytes", false);
    }

    @Test
    public void rowTests() {
        this.qst("""
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
        this.qst("""
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
        this.qst("""
                -- [CALCITE-922] Value of INTERVAL literal
                select deptno * interval '2' day as d2,
                 deptno * interval -'3' hour as h3,
                 deptno * interval -'-4' hour as h4,
                 deptno * interval -'4:30' hour to minute as h4_5,
                 deptno * interval -'-1-3' year to month as y1_25,
                 CAST(deptno AS DECIMAL(6, 2)) / 10 * interval 1 minutes as m,
                 CAST(deptno AS REAL) / 15 * interval 1 day as d
                from dept;
                +---------+------------+-----------+------------+--------------------+--------+--------------------+
                | D2      | H3         | H4        | H4_5       | Y1_25              | M      | D                  |
                +---------+------------+-----------+------------+--------------------+--------+--------------------+
                | 20 days |  -30 hours |  40 hours |  -45 hours | 12 years 06 months | 1 mins |  57600.001716 secs |
                | 40 days |  -60 hours |  80 hours |  -90 hours | 25 years 00 months | 2 mins | 115200.003433 secs |
                | 60 days |  -90 hours | 120 hours | -135 hours | 37 years 06 months | 3 mins | 172800 secs        |
                | 80 days | -120 hours | 160 hours | -180 hours | 50 years 00 months | 4 mins | 230400.006866 secs |
                +---------+------------+-----------+------------+--------------------+--------+--------------------+
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
        this.qst("""
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