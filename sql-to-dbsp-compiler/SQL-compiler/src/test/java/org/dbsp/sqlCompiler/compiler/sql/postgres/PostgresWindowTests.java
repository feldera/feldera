package org.dbsp.sqlCompiler.compiler.sql.postgres;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest.TestOptimizations.Optimized;

// postgres/src/test/regress/expected/window.out
public class PostgresWindowTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        compiler.submitStatementsForCompilation("""
                CREATE TABLE empsalary (
                    depname varchar,
                    empno bigint,
                    salary int,
                    enroll_date date
                );

                INSERT INTO empsalary VALUES
                ('develop', 10, 5200, '2007-08-01'),
                ('sales', 1, 5000, '2006-10-01'),
                ('personnel', 5, 3500, '2007-12-10'),
                ('sales', 4, 4800, '2007-08-08'),
                ('personnel', 2, 3900, '2006-12-23'),
                ('develop', 7, 4200, '2008-01-01'),
                ('develop', 9, 4500, '2008-01-01'),
                ('sales', 3, 4800, '2007-08-01'),
                ('develop', 8, 6000, '2006-10-01'),
                ('develop', 11, 5200, '2007-08-15');

                CREATE TABLE series(x int NOT NULL);
                INSERT INTO series VALUES
                (1),
                (2),
                (3),
                (4),
                (5),
                (6),
                (7),
                (8),
                (9),
                (10);

                --CREATE TABLE tenk1 (
                --	unique1		int4,
                --	unique2		int4,
                --	two			int4,
                --	four		int4,
                --	ten			int4,
                --	twenty		int4,
                --	hundred		int4,
                --	thousand	int4,
                --	twothousand	int4,
                --	fivethous	int4,
                --	tenthous	int4,
                --	odd			int4,
                --	even		int4,
                --	stringu1	varchar,
                --	stringu2	varchar,
                --	string4		varchar
                --);
                --
                -- smaller version, only contains values where unique2 < 10
                CREATE TABLE tenk1_2_small (
                	unique1		int4,
                	unique2		int4,
                	two			int4,
                	four		int4,
                	ten			int4,
                	twenty		int4,
                	hundred		int4,
                	thousand	int4,
                	twothousand	int4,
                	fivethous	int4,
                	tenthous	int4,
                	odd			int4,
                	even		int4,
                	stringu1	varchar,
                	stringu2	varchar,
                	string4		varchar
                );

                -- smaller version, only contains values where unique1 < 10
                CREATE TABLE tenk1_1_small (
                	unique1		int4,
                	unique2		int4,
                	two			int4,
                	four		int4,
                	ten			int4,
                	twenty		int4,
                	hundred		int4,
                	thousand	int4,
                	twothousand	int4,
                	fivethous	int4,
                	tenthous	int4,
                	odd			int4,
                	even		int4,
                	stringu1	varchar,
                	stringu2	varchar,
                	string4		varchar
                );

                create table t1 (f1 int, f2 int8);
                insert into t1 values (1,1),(1,2),(2,2);
                """);
        //this.insertFromResource("tenk1", compiler);
        this.insertFromResource("tenk1_1_small", compiler);
        this.insertFromResource("tenk1_2_small", compiler);
    }

    @Test
    public void testWindow() {
        this.qs("""
                SELECT depname, empno, salary, sum(salary)
                OVER (PARTITION BY depname)
                FROM empsalary -- ORDER BY depname, salary
                ;
                  depname  | empno | salary |  sum
                -----------+-------+--------+-------
                 develop   |     7 |   4200 | 25100
                 develop   |     9 |   4500 | 25100
                 develop   |    11 |   5200 | 25100
                 develop   |    10 |   5200 | 25100
                 develop   |     8 |   6000 | 25100
                 personnel |     5 |   3500 |  7400
                 personnel |     2 |   3900 |  7400
                 sales     |     3 |   4800 | 14600
                 sales     |     4 |   4800 | 14600
                 sales     |     1 |   5000 | 14600
                (10 rows)""");
    }

    @Test
    public void testLeadLag() {
        this.qs("""
                SELECT lag(ten) OVER (PARTITION BY four ORDER BY ten), ten, four
                FROM tenk1_2_small WHERE unique2 < 10;
                 lag | ten | four
                -----+-----+------
                     |   0 |    0
                   0 |   0 |    0
                   0 |   4 |    0
                     |   1 |    1
                   1 |   1 |    1
                   1 |   7 |    1
                   7 |   9 |    1
                     |   0 |    2
                     |   1 |    3
                   1 |   3 |    3
                (10 rows)

                SELECT lead(ten) OVER (PARTITION BY four ORDER BY ten), ten, four
                FROM tenk1_2_small
                WHERE unique2 < 10;
                 lead | ten | four
                ------+-----+------
                    0 |   0 |    0
                    4 |   0 |    0
                      |   4 |    0
                    1 |   1 |    1
                    7 |   1 |    1
                    9 |   7 |    1
                      |   9 |    1
                      |   0 |    2
                    3 |   1 |    3
                      |   3 |    3
                (10 rows)

                SELECT lead(ten * 2, 1) OVER (PARTITION BY four ORDER BY ten), ten, four
                FROM tenk1_2_small
                WHERE unique2 < 10;
                 lead | ten | four
                ------+-----+------
                    0 |   0 |    0
                    8 |   0 |    0
                      |   4 |    0
                    2 |   1 |    1
                   14 |   1 |    1
                   18 |   7 |    1
                      |   9 |    1
                      |   0 |    2
                    6 |   1 |    3
                      |   3 |    3
                (10 rows)

                SELECT lead(ten * 2, 1, -1) OVER (PARTITION BY four ORDER BY ten), ten, four
                FROM tenk1_2_small
                WHERE unique2 < 10;
                 lead | ten | four
                ------+-----+------
                    0 |   0 |    0
                    8 |   0 |    0
                   -1 |   4 |    0
                    2 |   1 |    1
                   14 |   1 |    1
                   18 |   7 |    1
                   -1 |   9 |    1
                   -1 |   0 |    2
                    6 |   1 |    3
                   -1 |   3 |    3
                (10 rows)""");
    }

    @Test
    public void testPreceding() {
        this.qs("""
                SELECT sum(unique1) over (order by four range between 2::int8 preceding and 1::int2 preceding),
                	unique1, four
                FROM tenk1_1_small WHERE unique1 < 10;
                 sum | unique1 | four
                -----+---------+------
                     |       0 |    0
                     |       8 |    0
                     |       4 |    0
                  12 |       5 |    1
                  12 |       9 |    1
                  12 |       1 |    1
                  27 |       6 |    2
                  27 |       2 |    2
                  23 |       3 |    3
                  23 |       7 |    3
                (10 rows)

                SELECT sum(unique1) over (partition by four order by unique1 range between 5::int8 preceding and 6::int2 following),
                	unique1, four
                FROM tenk1_1_small WHERE unique1 < 10;
                 sum | unique1 | four
                -----+---------+------
                   4 |       0 |    0
                  12 |       4 |    0
                  12 |       8 |    0
                   6 |       1 |    1
                  15 |       5 |    1
                  14 |       9 |    1
                   8 |       2 |    2
                   8 |       6 |    2
                  10 |       3 |    3
                  10 |       7 |    3
                (10 rows)
                """);
    }

    @Test
    public void testWindowDescOrder() {
        this.qs("""
                SELECT sum(unique1) over (order by four desc range between 2::int8 preceding and 1::int2 preceding),
                	unique1, four
                FROM tenk1_1_small WHERE unique1 < 10;
                 sum | unique1 | four
                -----+---------+------
                     |       3 |    3
                     |       7 |    3
                  10 |       6 |    2
                  10 |       2 |    2
                  18 |       9 |    1
                  18 |       5 |    1
                  18 |       1 |    1
                  23 |       0 |    0
                  23 |       8 |    0
                  23 |       4 |    0
                (10 rows)""");
    }

    @Test
    public void dateWindow() {
        // around line 1534
        // Converted INTERVAL 1 YEAR to INTERVAL 365 DAYS
        this.qs("""
                select sum(salary)
                OVER (order by enroll_date range between INTERVAL 365 DAYS preceding and INTERVAL 365 DAYS following),
                	salary, enroll_date FROM empsalary;
                  sum  | salary | enroll_date
                -------+--------+-------------
                 34900 |   5000 | 10-01-2006
                 34900 |   6000 | 10-01-2006
                 38400 |   3900 | 12-23-2006
                 47100 |   4800 | 08-01-2007
                 47100 |   5200 | 08-01-2007
                 47100 |   4800 | 08-08-2007
                 47100 |   5200 | 08-15-2007
                 36100 |   3500 | 12-10-2007
                 32200 |   4500 | 01-01-2008
                 32200 |   4200 | 01-01-2008
                (10 rows)

                select sum(salary)
                OVER (order by enroll_date desc range between INTERVAL 365 DAYS following and INTERVAL 365 DAYS following),
                	salary, enroll_date from empsalary;
                 sum | salary | enroll_date
                -----+--------+-------------
                     |   4200 | 01-01-2008
                     |   4500 | 01-01-2008
                     |   3500 | 12-10-2007
                     |   5200 | 08-15-2007
                     |   4800 | 08-08-2007
                     |   4800 | 08-01-2007
                     |   5200 | 08-01-2007
                     |   3900 | 12-23-2006
                     |   5000 | 10-01-2006
                     |   6000 | 10-01-2006
                (10 rows)""", Optimized);
        // This doesn't work unoptimized, because the constant window bound
        // is pulled out of the window operator.
    }

    @Test
    public void degenerateCases() {
        this.qs("""
                select f1, sum(f1) over (partition by f1 order by f2
                                         range between 1 preceding and 1 following)
                from t1 where f1 = f2;
                 f1 | sum
                ----+-----
                  1 |   1
                  2 |   2
                (2 rows)

                select f1, sum(f1) over (partition by f1, f1 order by f2
                                         range between 2 preceding and 1 preceding)
                from t1 where f1 = f2;
                 f1 | sum
                ----+-----
                  1 |
                  2 |
                (2 rows)

                select f1, sum(f1) over (partition by f1, f2 order by f2
                                         range between 1 following and 2 following)
                from t1 where f1 = f2;
                 f1 | sum
                ----+-----
                  1 |
                  2 |
                (2 rows)""");
    }

    @Test
    public void testTopK() {
        this.qs("""
                SELECT * FROM
                  (SELECT empno,
                          row_number() OVER (ORDER BY empno) rn
                   FROM empsalary) emp
                WHERE rn < 3;
                 empno | rn
                -------+----
                     1 |  1
                     2 |  2
                (2 rows)

                SELECT * FROM
                  (SELECT empno,
                          row_number() OVER (ORDER BY empno) rn
                   FROM empsalary) emp
                WHERE 3 > rn;
                 empno | rn
                -------+----
                     1 |  1
                     2 |  2
                (2 rows)

                SELECT * FROM
                  (SELECT empno,
                          row_number() OVER (ORDER BY empno) rn
                   FROM empsalary) emp
                WHERE 2 >= rn;
                 empno | rn
                -------+----
                     1 |  1
                     2 |  2
                (2 rows)

                SELECT * FROM
                  (SELECT empno,
                          salary,
                          rank() OVER (ORDER BY salary DESC) r
                   FROM empsalary) emp
                WHERE r <= 3;
                 empno | salary | r
                -------+--------+---
                     8 |   6000 | 1
                    10 |   5200 | 2
                    11 |   5200 | 2
                (3 rows)

                SELECT * FROM
                  (SELECT empno,
                          salary,
                          dense_rank() OVER (ORDER BY salary DESC) dr
                   FROM empsalary) emp
                WHERE dr = 1;
                 empno | salary | dr
                -------+--------+----
                     8 |   6000 |  1
                (1 row)

                SELECT * FROM
                  (SELECT empno,
                          salary,
                          count(*) OVER (ORDER BY salary DESC) c
                   FROM empsalary) emp
                WHERE c <= 3;
                 empno | salary | c
                -------+--------+---
                     8 |   6000 | 1
                    10 |   5200 | 3
                    11 |   5200 | 3
                (3 rows)

                SELECT * FROM
                  (SELECT empno,
                          salary,
                          count(empno) OVER (ORDER BY salary DESC) c
                   FROM empsalary) emp
                WHERE c <= 3;
                 empno | salary | c
                -------+--------+---
                     8 |   6000 | 1
                    10 |   5200 | 3
                    11 |   5200 | 3
                (3 rows)

                SELECT * FROM
                  (SELECT empno,
                          depname,
                          row_number() OVER (PARTITION BY depname ORDER BY empno) rn
                   FROM empsalary) emp
                WHERE rn < 3;
                 empno |  depname  | rn
                -------+-----------+----
                     7 | develop|     1
                     8 | develop|     2
                     2 | personnel|   1
                     5 | personnel|   2
                     1 | sales|       1
                     3 | sales|       2
                (6 rows)

                SELECT * FROM
                  (SELECT empno,
                          depname,
                          salary,
                          count(empno) OVER (PARTITION BY depname ORDER BY salary DESC) c
                   FROM empsalary) emp
                WHERE c <= 3;
                 empno |  depname  | salary | c
                -------+-----------+--------+---
                     8 | develop|      6000 | 1
                    10 | develop|      5200 | 3
                    11 | develop|      5200 | 3
                     2 | personnel|    3900 | 1
                     5 | personnel|    3500 | 2
                     1 | sales|        5000 | 1
                     4 | sales|        4800 | 3
                     3 | sales|        4800 | 3
                (8 rows)
                """);
    }

    @Test @Ignore("Lead with variable amounts not supported")
    public void testLeadLagVariable() {
        this.qs("""
                select x, lag(x, 1) over (order by x), lead(x, 3) over (order by x)
                from series;
                 x  | lag | lead
                ----+-----+------
                  1 |     |    4
                  2 |   1 |    5
                  3 |   2 |    6
                  4 |   3 |    7
                  5 |   4 |    8
                  6 |   5 |    9
                  7 |   6 |   10
                  8 |   7 |
                  9 |   8 |
                 10 |   9 |
                (10 rows)

                SELECT lag(ten, four) OVER (PARTITION BY four ORDER BY ten), ten, four
                FROM tenk1_2_small
                WHERE unique2 < 10;
                 lag | ten | four
                -----+-----+------
                   0 |   0 |    0
                   0 |   0 |    0
                   4 |   4 |    0
                     |   1 |    1
                   1 |   1 |    1
                   1 |   7 |    1
                   7 |   9 |    1
                     |   0 |    2
                     |   1 |    3
                     |   3 |    3
                (10 rows)

                SELECT lag(ten, four, 0) OVER (PARTITION BY four ORDER BY ten), ten, four
                FROM tenk1_2_small
                WHERE unique2 < 10;
                 lag | ten | four
                -----+-----+------
                   0 |   0 |    0
                   0 |   0 |    0
                   4 |   4 |    0
                   0 |   1 |    1
                   1 |   1 |    1
                   1 |   7 |    1
                   7 |   9 |    1
                   0 |   0 |    2
                   0 |   1 |    3
                   0 |   3 |    3
                (10 rows)

                SELECT lag(ten, four, 0.7) OVER (PARTITION BY four ORDER BY ten), ten, four
                FROM tenk1_2_small
                WHERE unique2 < 10 ORDER BY four, ten;
                 lag | ten | four
                -----+-----+------
                   0 |   0 |    0
                   0 |   0 |    0
                   4 |   4 |    0
                 0.7 |   1 |    1
                   1 |   1 |    1
                   1 |   7 |    1
                   7 |   9 |    1
                 0.7 |   0 |    2
                 0.7 |   1 |    3
                 0.7 |   3 |    3
                (10 rows)""");
    }
}
