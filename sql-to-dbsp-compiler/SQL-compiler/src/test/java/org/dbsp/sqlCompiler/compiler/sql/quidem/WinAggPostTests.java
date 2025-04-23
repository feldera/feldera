package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Ignore;
import org.junit.Test;

// Test from winagg.iq that use the Post database
public class WinAggPostTests extends PostBaseTests {
    @Test @Ignore("ORDER BY strings not supported https://github.com/feldera/feldera/issues/457")
    public void test() {
        this.qs("""
                select deptno,
                  ename,
                  mode(gender) over (partition by deptno order by ENAME) as m
                from emp;
                +--------+-------+---+
                | DEPTNO | ENAME | M |
                +--------+-------+---+
                |     10 | Bob   | M |
                |     10 | Jane  | M |
                |     20 | Eric  | M |
                |     30 | Alice | F |
                |     30 | Susan | F |
                |     50 | Adam  | M |
                |     50 | Eve   | M |
                |     60 | Grace | F |
                |        | Wilma | F |
                +--------+-------+---+
                (9 rows)
                
                -- [CALCITE-5283] Add ARG_MIN, ARG_MAX aggregate function
                -- ARG_MIN, ARG_MAX function without ORDER BY.
                select gender,
                  arg_min(ename, deptno) over (partition by gender order by ename) as mi,
                  arg_max(ename, deptno) over (partition by gender order by ename) as ma
                from emp;
                +--------+-------+-------+
                | GENDER | MI    | MA    |
                +--------+-------+-------+
                | F      | Alice | Alice |
                | F      | Alice | Eve   |
                | F      | Alice | Grace |
                | F      | Jane  | Grace |
                | F      | Jane  | Grace |
                | F      | Jane  | Grace |
                | M      | Adam  | Adam  |
                | M      | Bob   | Adam  |
                | M      | Bob   | Adam  |
                +--------+-------+-------+
                (9 rows)

                -- Multiple window functions sharing a single window
                select count(*) over(partition by gender order by ename) as count1,
                  count(*) over(partition by deptno order by ename) as count2,
                  sum(deptno) over(partition by gender order by ename) as sum1,
                  sum(deptno) over(partition by deptno order by ename) as sum2
                from emp
                order by sum1, sum2;
                +--------+--------+------+------+
                | COUNT1 | COUNT2 | SUM1 | SUM2 |
                +--------+--------+------+------+
                |      1 |      1 |   30 |   30 |
                |      1 |      1 |   50 |   50 |
                |      2 |      1 |   60 |   10 |
                |      3 |      1 |   80 |   20 |
                |      2 |      2 |   80 |  100 |
                |      3 |      1 |  140 |   60 |
                |      4 |      2 |  150 |   20 |
                |      5 |      2 |  180 |   60 |
                |      6 |      1 |  180 |      |
                +--------+--------+------+------+
                (9 rows)
                
                -- Window Aggregate and group-by.
                select min(deptno) as x, rank() over (order by ename) as y,
                  max(ename) over (partition by deptno) as z
                from emp
                group by deptno, ename;
                +----+---+-------+
                | X  | Y | Z     |
                +----+---+-------+
                |    | 9 | Wilma |
                | 50 | 1 | Eve   |
                | 50 | 5 | Eve   |
                | 20 | 4 | Eric  |
                | 10 | 3 | Jane  |
                | 10 | 7 | Jane  |
                | 60 | 6 | Grace |
                | 30 | 2 | Susan |
                | 30 | 8 | Susan |
                +----+---+-------+
                (9 rows)

                -- Window function on top of regular aggregate in partitioning or order clause.
                select deptno, gender, min(ename) as x, sum(deptno) as y,
                  rank() over (partition by gender order by min(ename)) as r,
                  sum(sum(deptno)) over (partition by gender order by min(ename)) as s
                from emp
                group by deptno, gender
                order by gender, r;
                +--------+--------+-------+----+---+-----+
                | DEPTNO | GENDER | X     | Y  | R | S   |
                +--------+--------+-------+----+---+-----+
                |     30 | F      | Alice | 60 | 1 |  60 |
                |     50 | F      | Eve   | 50 | 2 | 110 |
                |     60 | F      | Grace | 60 | 3 | 170 |
                |     10 | F      | Jane  | 10 | 4 | 180 |
                |        | F      | Wilma |    | 5 | 180 |
                |     50 | M      | Adam  | 50 | 1 |  50 |
                |     10 | M      | Bob   | 10 | 2 |  60 |
                |     20 | M      | Eric  | 20 | 3 |  80 |
                +--------+--------+-------+----+---+-----+
                (8 rows)

                select *, first_value(ename) over (partition by deptno order by gender range unbounded preceding) from emp;
                 ename | deptno | gender | first_value
                -------+--------+--------+-------------
                 Jane  |     10 | F      | Jane
                 Bob   |     10 | M      | Jane
                 Eric  |     20 | M      | Eric
                 Alice |     30 | F      | Alice
                 Susan |     30 | F      | Alice
                 Eve   |     50 | F      | Eve
                 Adam  |     50 | M      | Eve
                 Grace |     60 | F      | Grace
                (8 rows)

                -- [CALCITE-6011] Add the planner rule that pushes the Filter past a Window
                -- Get the initial result which not push filter past window.
                select gender, count(*) over(partition by gender order by ename) as count1 from emp;
                +--------+--------+
                | GENDER | COUNT1 |
                +--------+--------+
                | F      |      1 |
                | F      |      2 |
                | F      |      3 |
                | F      |      4 |
                | F      |      5 |
                | F      |      6 |
                | M      |      1 |
                | M      |      2 |
                | M      |      3 |
                +--------+--------+
                (9 rows)

                -- Get the plan and result which push filter past window
                select gender, count(*) over(partition by gender order by ename) as count1 from emp where gender = 'F';
                +--------+--------+
                | GENDER | COUNT1 |
                +--------+--------+
                | F      |      1 |
                | F      |      2 |
                | F      |      3 |
                | F      |      4 |
                | F      |      5 |
                | F      |      6 |
                +--------+--------+
                (6 rows)""");
    }

    @Test
    public void test0() {
        this.qs("""
                -- [CALCITE-1540] Support multiple columns in PARTITION BY clause of window function
                select gender,deptno,
                  count(*) over (partition by gender,deptno) as count1
                from emp;
                +--------+--------+--------+
                | GENDER | DEPTNO | COUNT1 |
                +--------+--------+--------+
                | F|           10 |      1 |
                | F|           30 |      2 |
                | F|           30 |      2 |
                | F|           50 |      1 |
                | F|           60 |      1 |
                | F|              |      1 |
                | M|           10 |      1 |
                | M|           20 |      1 |
                | M|           50 |      1 |
                +--------+--------+--------+
                (9 rows)

                -- Partition by same column twice.
                -- (It's degenerate, but I believe it's valid SQL.)
                select gender,deptno,
                  count(*) over (partition by gender,gender) as count1
                from emp;
                +--------+--------+--------+
                | GENDER | DEPTNO | COUNT1 |
                +--------+--------+--------+
                | F|           10 |      6 |
                | F|           30 |      6 |
                | F|           30 |      6 |
                | F|           50 |      6 |
                | F|           60 |      6 |
                | F|              |      6 |
                | M|           10 |      3 |
                | M|           20 |      3 |
                | M|           50 |      3 |
                +--------+--------+--------+
                (9 rows)""");
    }

    @Test
    public void test2() {
        // These tests are non-deterministic in SQL; they are
        // deterministic in our implementation, but they give a different result
        // than other SQL dialects.
        // Here the sorting is implicit on ename, deptno, gender
        this.qs("""
                select *, first_value(deptno) over () from emp;
                 ename | deptno | gender | first_value
                -------+--------+--------+-------------
                 Jane  |     10 | F|                50
                 Bob   |     10 | M|                50
                 Eric  |     20 | M|                50
                 Susan |     30 | F|                50
                 Alice |     30 | F|                50
                 Adam  |     50 | M|                50
                 Eve   |     50 | F|                50
                 Grace |     60 | F|                50
                 Wilma |        | F|                50
                (9 rows)""");
        this.qs("""
                select *, first_value(ename) over () from emp;
                 ename | deptno | gender | first_value
                -------+--------+--------+-------------
                 Jane  |     10 | F| Adam
                 Bob   |     10 | M| Adam
                 Eric  |     20 | M| Adam
                 Susan |     30 | F| Adam
                 Alice |     30 | F| Adam
                 Adam  |     50 | M| Adam
                 Eve   |     50 | F| Adam
                 Grace |     60 | F| Adam
                 Wilma |        | F| Adam
                (9 rows)

                select *, first_value(ename) over (partition by deptno) from emp;
                 ename | deptno | gender | first_value
                -------+--------+--------+-------------
                 Jane  |     10 | F| Bob
                 Bob   |     10 | M| Bob
                 Eric  |     20 | M| Eric
                 Susan |     30 | F| Alice
                 Alice |     30 | F| Alice
                 Adam  |     50 | M| Adam
                 Eve   |     50 | F| Adam
                 Grace |     60 | F| Grace
                 Wilma |        | F| Wilma
                (9 rows)""");
    }

    @Test
    public void test3() {
        this.qs("""
                -- No ORDER BY, windows defined in WINDOW clause.
                select deptno, gender, min(gender) over w1 as a, min(gender) over w2 as d
                from emp
                window w1 as (),
                 w2 as (partition by deptno);
                +--------+--------+---+---+
                | DEPTNO | GENDER | A | D |
                +--------+--------+---+---+
                |     10 | F| F| F|
                |     10 | M| F| F|
                |     20 | M| F| M|
                |     30 | F| F| F|
                |     30 | F| F| F|
                |     50 | F| F| F|
                |     50 | M| F| F|
                |     60 | F| F| F|
                |        | F| F| F|
                +--------+--------+---+---+
                (9 rows)

                -- Without ORDER BY
                select *, count(*) over (partition by deptno) as c from emp;
                +-------+--------+--------+---+
                | ENAME | DEPTNO | GENDER | C |
                +-------+--------+--------+---+
                | Adam  |     50 | M| 2       |
                | Alice |     30 | F| 2       |
                | Bob   |     10 | M| 2       |
                | Eric  |     20 | M| 1       |
                | Eve   |     50 | F| 2       |
                | Grace |     60 | F| 1       |
                | Jane  |     10 | F| 2       |
                | Susan |     30 | F| 2       |
                | Wilma |        | F| 1       |
                +-------+--------+--------+---+
                (9 rows)

                -- Composite COUNT.
                select deptno, gender, count(gender, deptno) over w1 as a
                from emp
                window w1 as ();
                +--------+--------+---+
                | DEPTNO | GENDER | A |
                +--------+--------+---+
                |     10 | F| 8       |
                |     10 | M| 8       |
                |     20 | M| 8       |
                |     30 | F| 8       |
                |     30 | F| 8       |
                |     50 | F| 8       |
                |     50 | M| 8       |
                |     60 | F| 8       |
                |        | F| 8       |
                +--------+--------+---+
                (9 rows)""");
    }

    @Test @Ignore("unsupported aggregate functions")
    public void test4() {
        this.qs("""
                -- NTH_VALUE
                select emp."ENAME", emp."DEPTNO",
                 nth_value(emp."DEPTNO", 1) over() as "first_value",
                 nth_value(emp."DEPTNO", 2) over() as "second_value",
                 nth_value(emp."DEPTNO", 5) over() as "fifth_value",
                 nth_value(emp."DEPTNO", 8) over() as "eighth_value",
                 nth_value(emp."DEPTNO", 10) over() as "tenth_value"
                from emp order by emp."ENAME";
                +-------+--------+-------------+--------------+-------------+--------------+-------------+
                | ENAME | DEPTNO | first_value | second_value | fifth_value | eighth_value | tenth_value |
                +-------+--------+-------------+--------------+-------------+--------------+-------------+
                | Adam  |     50 |          10 |           10 |          30 |           60 |             |
                | Alice |     30 |          10 |           10 |          30 |           60 |             |
                | Bob   |     10 |          10 |           10 |          30 |           60 |             |
                | Eric  |     20 |          10 |           10 |          30 |           60 |             |
                | Eve   |     50 |          10 |           10 |          30 |           60 |             |
                | Grace |     60 |          10 |           10 |          30 |           60 |             |
                | Jane  |     10 |          10 |           10 |          30 |           60 |             |
                | Susan |     30 |          10 |           10 |          30 |           60 |             |
                | Wilma |        |          10 |           10 |          30 |           60 |             |
                +-------+--------+-------------+--------------+-------------+--------------+-------------+
                (9 rows)

                -- [CALCITE-2402] COVAR_POP, REGR_COUNT functions
                -- SUM(x, y) = SUM(x) WHERE y IS NOT NULL
                -- COVAR_POP(x, y) = (SUM(x * y) - SUM(x, y) * SUM(y, x) / REGR_COUNT(x, y)) / REGR_COUNT(x, y)
                select emps."AGE", emps."DEPTNO",
                 sum(emps."AGE" * emps."DEPTNO") over() as "sum(age * deptno)",
                 regr_count(emps."AGE", emps."DEPTNO") over() as "regr_count(age, deptno)",
                 covar_pop(emps."DEPTNO", emps."AGE") over() as "covar_pop"
                from emps order by emps."AGE";
                +-----+--------+-------------------+-------------------------+-----------+
                | AGE | DEPTNO | sum(age * deptno) | regr_count(age, deptno) | covar_pop |
                +-----+--------+-------------------+-------------------------+-----------+
                |   5 |     20 |              1950 |                       3 |        39 |
                |  25 |     10 |              1950 |                       3 |        39 |
                |  80 |     20 |              1950 |                       3 |        39 |
                |     |     40 |              1950 |                       3 |        39 |
                |     |     40 |              1950 |                       3 |        39 |
                +-----+--------+-------------------+-------------------------+-----------+
                (5 rows)

                -- [CALCITE-2402] COVAR_POP, REGR_COUNT functions
                -- SUM(x, y) = SUM(x) WHERE y IS NOT NULL
                -- COVAR_POP(x, y) = (SUM(x * y) - SUM(x, y) * SUM(y, x) / REGR_COUNT(x, y)) / REGR_COUNT(x, y)
                select emps."AGE", emps."DEPTNO", emps."GENDER",
                 sum(emps."AGE" * emps."DEPTNO") over(partition by emps."GENDER") as "sum(age * deptno)",
                 regr_count(emps."AGE", emps."DEPTNO") over(partition by emps."GENDER") as "regr_count(age, deptno)",
                 covar_pop(emps."DEPTNO", emps."AGE") over(partition by emps."GENDER") as "covar_pop"
                from emps order by emps."GENDER";
                +-----+--------+--------+-------------------+-------------------------+-----------+
                | AGE | DEPTNO | GENDER | sum(age * deptno) | regr_count(age, deptno) | covar_pop |
                +-----+--------+--------+-------------------+-------------------------+-----------+
                |   5 |     20 | F      |               100 |                       1 |         0 |
                |     |     40 | F      |               100 |                       1 |         0 |
                |  80 |     20 | M      |              1600 |                       1 |         0 |
                |     |     40 | M      |              1600 |                       1 |         0 |
                |  25 |     10 |        |               250 |                       1 |         0 |
                +-----+--------+--------+-------------------+-------------------------+-----------+
                (5 rows)

                -- [CALCITE-2402] COVAR_SAMP functions
                -- SUM(x, y) = SUM(x) WHERE y IS NOT NULL
                -- COVAR_SAMP(x, y) = (SUM(x * y) - SUM(x, y) * SUM(y, x) / REGR_COUNT(x, y)) / (REGR_COUNT(x, y) - 1)
                select emps."AGE", emps."DEPTNO", emps."GENDER",
                 covar_samp(emps."AGE", emps."AGE") over() as "var_samp",
                 covar_samp(emps."DEPTNO", emps."AGE") over() as "covar_samp",
                 covar_samp(emps."EMPNO", emps."DEPTNO") over(partition by emps."MANAGER") as "covar_samp partitioned"
                from emps order by emps."AGE";
                +-----+--------+--------+----------+------------+------------------------+
                | AGE | DEPTNO | GENDER | var_samp | covar_samp | covar_samp partitioned |
                +-----+--------+--------+----------+------------+------------------------+
                |   5 |     20 | F      |     1508 |         58 |                      0 |
                |  25 |     10 |        |     1508 |         58 |                     50 |
                |  80 |     20 | M      |     1508 |         58 |                     50 |
                |     |     40 | M      |     1508 |         58 |                      0 |
                |     |     40 | F      |     1508 |         58 |                      0 |
                +-----+--------+--------+----------+------------+------------------------+
                (5 rows)

                -- [CALCITE-2402] VAR_POP, VAR_SAMP functions
                -- VAR_POP(x) = (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x)) / COUNT(x)
                -- VAR_SAMP(x) = (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x)) / (COUNT(x) - 1)
                select emps."AGE", emps."DEPTNO", emps."GENDER",
                 var_pop(emps."AGE") over() as "var_pop",
                 var_pop(emps."AGE") over(partition by emps."AGE") as "var_pop by age",
                 var_samp(emps."AGE") over() as "var_samp",
                 var_samp(emps."AGE") over(partition by emps."GENDER") as "var_samp by gender"
                from emps order by emps."AGE";
                +-----+--------+--------+---------+----------------+----------+--------------------+
                | AGE | DEPTNO | GENDER | var_pop | var_pop by age | var_samp | var_samp by gender |
                +-----+--------+--------+---------+----------------+----------+--------------------+
                |   5 |     20 | F      |    1005 |              0 |     1508 |                    |
                |  25 |     10 |        |    1005 |              0 |     1508 |                    |
                |  80 |     20 | M      |    1005 |              0 |     1508 |                    |
                |     |     40 | F      |    1005 |                |     1508 |                    |
                |     |     40 | M      |    1005 |                |     1508 |                    |
                +-----+--------+--------+---------+----------------+----------+--------------------+
                (5 rows)

                -- [CALCITE-2402] REGR_SXX, REGR_SXY, REGR_SYY functions
                -- SUM(x, y) = SUM(x) WHERE y IS NOT NULL
                -- REGR_SXX(x, y) = REGR_COUNT(x, y) * VAR_POP(y, y)
                -- REGR_SXY(x, y) = REGR_COUNT(x, y) * COVAR_POP(x, y)
                -- REGR_SYY(x, y) = REGR_COUNT(x, y) * VAR_POP(x, x)
                ---- COVAR_POP(x, y) = (SUM(x * y) - SUM(x, y) * SUM(y, x) / REGR_COUNT(x, y)) / REGR_COUNT(x, y)
                ---- VAR_POP(y, y) = (SUM(y * y, x) - SUM(y, x) * SUM(y, x) / REGR_COUNT(x, y)) / REGR_COUNT(x, y)
                select emps."AGE", emps."DEPTNO",
                 regr_sxx(emps."AGE", emps."DEPTNO") over() as "regr_sxx(age, deptno)",
                 regr_syy(emps."AGE", emps."DEPTNO") over() as "regr_syy(age, deptno)"
                from emps order by emps."AGE";
                +-----+--------+-----------------------+-----------------------+
                | AGE | DEPTNO | regr_sxx(age, deptno) | regr_syy(age, deptno) |
                +-----+--------+-----------------------+-----------------------+
                |   5 |     20 |                    66 |                  3015 |
                |  25 |     10 |                    66 |                  3015 |
                |  80 |     20 |                    66 |                  3015 |
                |     |     40 |                    66 |                  3015 |
                |     |     40 |                    66 |                  3015 |
                +-----+--------+-----------------------+-----------------------+
                (5 rows)

                -- [CALCITE-2402] REGR_SXX, REGR_SXY, REGR_SYY functions
                -- SUM(x, y) = SUM(x) WHERE y IS NOT NULL
                -- REGR_SXX(x, y) = REGR_COUNT(x, y) * COVAR_POP(y, y)
                -- REGR_SXY(x, y) = REGR_COUNT(x, y) * COVAR_POP(x, y)
                -- REGR_SYY(x, y) = REGR_COUNT(x, y) * COVAR_POP(x, x)
                ---- COVAR_POP(x, y) = (SUM(x * y) - SUM(x, y) * SUM(y, x) / REGR_COUNT(x, y)) / REGR_COUNT(x, y)
                ---- COVAR_POP(y, y) = (SUM(y * y, x) - SUM(y, x) * SUM(y, x) / REGR_COUNT(x, y)) / REGR_COUNT(x, y)
                select emps."AGE", emps."DEPTNO", emps."GENDER",
                 regr_sxx(emps."AGE", emps."DEPTNO") over(partition by emps."GENDER") as "regr_sxx(age, deptno)",
                 regr_syy(emps."AGE", emps."DEPTNO") over(partition by emps."GENDER") as "regr_syy(age, deptno)"
                from emps order by emps."GENDER";
                +-----+--------+--------+-----------------------+-----------------------+
                | AGE | DEPTNO | GENDER | regr_sxx(age, deptno) | regr_syy(age, deptno) |
                +-----+--------+--------+-----------------------+-----------------------+
                |   5 |     20 | F      |                     0 |                     0 |
                |     |     40 | F      |                     0 |                     0 |
                |  80 |     20 | M      |                     0 |                     0 |
                |     |     40 | M      |                     0 |                     0 |
                |  25 |     10 |        |                     0 |                     0 |
                +-----+--------+--------+-----------------------+-----------------------+
                (5 rows)

                -- [CALCITE-3661] MODE function
                -- MODE function without ORDER BY.
                select deptno,
                  mode(gender) over (partition by deptno) as m
                from emp;
                +--------+---+
                | DEPTNO | M |
                +--------+---+
                |     10 | F |
                |     10 | F |
                |     20 | M |
                |     30 | F |
                |     30 | F |
                |     50 | M |
                |     50 | M |
                |     60 | F |
                |        | F |
                +--------+---+
                (9 rows)""");
    }

    @Test
    public void testWindows1() {
        // Adjusted and validated using MySQL
        this.qs("""
                select *, count(*) over (order by deptno) as c from emp;
                 ENAME | DEPTNO | GENDER | C
                -------+--------+--------+---
                 Jane  |     10 | F| 3
                 Bob   |     10 | M| 3
                 Eric  |     20 | M| 4
                 Susan |     30 | F| 6
                 Alice |     30 | F| 6
                 Adam  |     50 | M| 8
                 Eve   |     50 | F| 8
                 Grace |     60 | F| 9
                 Wilma |        | F| 1
                (9 rows)""");
    }

    @Test @Ignore("RANK not supported")
    public void testWindows2() {
        this.qs("""
                select *, rank() over (order by deptno) as c from emp;
                +-------+--------+--------+---+
                | ENAME | DEPTNO | GENDER | C |
                +-------+--------+--------+---+
                | Adam  |     50 | M      | 6 |
                | Alice |     30 | F      | 4 |
                | Bob   |     10 | M      | 1 |
                | Eric  |     20 | M      | 3 |
                | Eve   |     50 | F      | 6 |
                | Grace |     60 | F      | 8 |
                | Jane  |     10 | F      | 1 |
                | Susan |     30 | F      | 4 |
                | Wilma |        | F      | 9 |
                +-------+--------+--------+---+
                (9 rows)

                -- Calcite does not yet generate tied ranks
                select *, dense_rank() over (order by deptno) as c from emp;
                +-------+--------+--------+---+
                | ENAME | DEPTNO | GENDER | C |
                +-------+--------+--------+---+
                | Adam  |     50 | M      | 4 |
                | Alice |     30 | F      | 3 |
                | Bob   |     10 | M      | 1 |
                | Eric  |     20 | M      | 2 |
                | Eve   |     50 | F      | 4 |
                | Grace |     60 | F      | 5 |
                | Jane  |     10 | F      | 1 |
                | Susan |     30 | F      | 3 |
                | Wilma |        | F      | 6 |
                +-------+--------+--------+---+
                (9 rows)

                -- [CALCITE-806] ROW_NUMBER should emit distinct values
                --
                -- We only run this test under JDK 1.8 because the results are
                -- non-deterministic and are different (but still correct) on
                -- JDK 1.7 and other platforms.
                select *,
                  row_number() over (order by deptno) as r1,
                  row_number() over (partition by deptno order by gender desc) as r2,
                  row_number() over (partition by deptno order by gender) as r3,
                  row_number() over (partition by gender) as r4,
                  row_number() over () as r
                from emp;
                +-------+--------+--------+----+----+----+----+---+
                | ENAME | DEPTNO | GENDER | R1 | R2 | R3 | R4 | R |
                +-------+--------+--------+----+----+----+----+---+
                | Adam  |     50 | M      |  6 |  1 |  2 |  1 | 7 |
                | Alice |     30 | F      |  5 |  2 |  2 |  6 | 6 |
                | Bob   |     10 | M      |  2 |  1 |  2 |  3 | 9 |
                | Eric  |     20 | M      |  3 |  1 |  1 |  2 | 8 |
                | Eve   |     50 | F      |  7 |  2 |  1 |  2 | 2 |
                | Grace |     60 | F      |  8 |  1 |  1 |  4 | 4 |
                | Jane  |     10 | F      |  1 |  2 |  1 |  3 | 3 |
                | Susan |     30 | F      |  4 |  1 |  1 |  5 | 5 |
                | Wilma |        | F      |  9 |  1 |  1 |  1 | 1 |
                +-------+--------+--------+----+----+----+----+---+
                (9 rows)

                -- As above, ROW_NUMBER without explicit ORDER BY
                select deptno,
                  ename,
                  row_number() over (partition by deptno) as r
                from emp
                where gender = 'F';
                +--------+-------+---+
                | DEPTNO | ENAME | R |
                +--------+-------+---+
                |     10 | Jane  | 1 |
                |     30 | Alice | 2 |
                |     30 | Susan | 1 |
                |     50 | Eve   | 1 |
                |     60 | Grace | 1 |
                |        | Wilma | 1 |
                +--------+-------+---+
                (6 rows)""");
    }

    @Test @Ignore("MAP not yet supported")
    public void testWindows3() {
        this.qs("""
                -- [CALCITE-2271] Two windows under a JOIN 2
                select
                 t1.l, t1.key as key1, t2.key as key2
                from
                 (
                  select
                   dense_rank() over (order by key) l,
                   key
                  from
                   unnest(map[1,1,2,2]) k
                 ) t1
                 join
                 (
                  select
                   dense_rank() over(order by key) l,
                   key
                  from
                   unnest(map[2,2]) k
                 ) t2 on (t1.l = t2.l and t1.key + 1 = t2.key);
                +---+------+------+
                | L | KEY1 | KEY2 |
                +---+------+------+
                | 1 |    1 |    2 |
                +---+------+------+
                (1 row)""");
    }

    @Test
    public void testCountIf() {
        this.qs("""
                -- COUNTIF(b) (BigQuery) is equivalent to COUNT(*) FILTER (WHERE b)
                select deptno, countif(gender = 'F') as f
                from emp
                group by deptno;
                +--------+---+
                | DEPTNO | F |
                +--------+---+
                |     10 | 1 |
                |     20 | 0 |
                |     30 | 2 |
                |     50 | 1 |
                |     60 | 1 |
                |        | 1 |
                +--------+---+
                (6 rows)
                
                select countif(gender = 'F') filter (where deptno = 30) as f
                from emp;
                +---+
                | F |
                +---+
                | 2 |
                +---+
                (1 row)
                
                select countif(a > 0) + countif(a > 1) + countif(c > 1) as c
                from (select 1 as a, 2 as b, 3 as c);
                +---+
                | C |
                +---+
                | 2 |
                +---+
                (1 row)""");
    }

    @Test
    public void testGrouping3() {
        this.qs("""
                -- [CALCITE-4665] Allow Aggregate.groupKey to be a strict superset of
             -- Aggregate.groupKeys
             -- Use a condition on grouping_id to filter out the superset grouping sets.
             select ename, deptno, gender, grouping(ename) as g_e,
               grouping(deptno) as g_d, grouping(gender) as g_g
             from emp
             where gender = 'M'
             group by grouping sets (ename, deptno, (ename, deptno),
               (ename, deptno, gender))
             having grouping_id(ename, deptno, gender) <> 0
             order by ename, deptno;
             +-------+--------+--------+-----+-----+-----+
             | ENAME | DEPTNO | GENDER | G_E | G_D | G_G |
             +-------+--------+--------+-----+-----+-----+
             | Adam|       50 |NULL|       0 |   0 |   1 |
             | Adam|          |NULL|       0 |   1 |   1 |
             | Bob|        10 |NULL|       0 |   0 |   1 |
             | Bob|           |NULL|       0 |   1 |   1 |
             | Eric|       20 |NULL|       0 |   0 |   1 |
             | Eric|          |NULL|       0 |   1 |   1 |
             |NULL|        10 |NULL|       1 |   0 |   1 |
             |NULL|        20 |NULL|       1 |   0 |   1 |
             |NULL|        50 |NULL|       1 |   0 |   1 |
             +-------+--------+--------+-----+-----+-----+
             (9 rows)
             
             -- just a comparison about the above sql
             select ename, deptno, grouping(ename) as g_e,
               grouping(deptno) as g_d
             from emp
             where gender = 'M'
             group by grouping sets (ename, deptno, (ename, deptno))
             order by ename, deptno;
             +-------+--------+-----+-----+
             | ENAME | DEPTNO | G_E | G_D |
             +-------+--------+-----+-----+
             | Adam|       50 |   0 |   0 |
             | Adam|          |   0 |   1 |
             | Bob|        10 |   0 |   0 |
             | Bob|           |   0 |   1 |
             | Eric|       20 |   0 |   0 |
             | Eric|          |   0 |   1 |
             |NULL|        10 |   1 |   0 |
             |NULL|        20 |   1 |   0 |
             |NULL|        50 |   1 |   0 |
             +-------+--------+-----+-----+
             (9 rows)
             
             -- Test cases for [CALCITE-5209] Proper sub-query handling if it is used inside select list and group by
             select
                 case when deptno in (1, 2, 3, 4, 5) THEN 1 else 0 end
             from emp
             group by
                 case when deptno in (1, 2, 3, 4, 5) THEN 1 else 0 end;
             +--------+
             | EXPR$0 |
             +--------+
             |      0 |
             +--------+
             (1 row)
             
             select
                 case when deptno in (1, 2, 3, 4, 5) THEN 1 else 0 end
             from emp
             group by
                 case when deptno in (1, 2, 3, 4, 5) THEN 1 else 0 end;
             +--------+
             | EXPR$0 |
             +--------+
             |      0 |
             +--------+
             (1 row)""");
    }
}
