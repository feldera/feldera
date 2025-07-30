package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Ignore;
import org.junit.Test;

// https://github.com/apache/calcite/blob/main/core/src/test/resources/sql/agg.iq
public class AggTests extends PostBaseTests {
    @Test
    public void issue4278() {
        this.qf("""
                select ename
                from emp as r
                left join dept as s on (select true from emp)""",
                "More than one value in subquery", false);
    }

    @Test
    public void calciteIssue6520() {
        this.qs("""
                SELECT ENAME, ENAME in ('Adam', 'Alice', 'Eve') FROM EMP;
                 ename | expr
                --------------
                 Adam| true
                 Alice| true
                 Bob| false
                 Eric| false
                 Eve| true
                 Grace| false
                 Jane| false
                 Susan| false
                 Wilma| false
                ---------------
                (9 rows)""");
    }

    @Test
    public void testCompositeCount() {
        this.qs("""
                -- composite count
                select count(deptno, ename, 1, deptno) as c from emp;
                +---+
                | C |
                +---+
                | 8 |
                +---+
                (1 row)

                -- COUNT excludes fully or partially null rows
                select count(city, gender) as c from emps;
                +---+
                | C |
                +---+
                | 3 |
                +---+
                (1 row)

                -- COUNT-DISTINCT excludes fully or partially null rows
                select count(distinct city, gender) as c from emps;
                +---+
                | C |
                +---+
                | 3 |
                +---+
                (1 row)

                select distinct mod(deptno, 20) as m, gender as c from emps;
                +----+---+
                | M  | C |
                +----+---+
                |  0 | F|
                | 10 |NULL|
                |  0 | M|
                +----+---+
                (3 rows)

                -- Partially null row (10, NULL) is excluded from count.
                select count(distinct mod(deptno, 20), gender) as c from emps;
                +---+
                | C |
                +---+
                | 2 |
                +---+
                (1 row)

                select count(mod(deptno, 20), gender) as c from emps;
                +---+
                | C |
                +---+
                | 4 |
                +---+
                (1 row)""");
    }

    @Test
    public void testAggregates() {
        this.qs("""
                -- count(*) returns number of rows in table
                select count(ename) as c from emp;
                +---+
                | C |
                +---+
                | 9 |
                +---+
                (1 row)

                -- count of not-nullable column same as count(*)
                select count(ename) as c from emp;
                +---+
                | C |
                +---+
                | 9 |
                +---+
                (1 row)

                -- count of nullable column
                select count(deptno) as c from emp;
                +---+
                | C |
                +---+
                | 8 |
                +---+
                (1 row)

                -- DISTINCT and GROUP BY
                select distinct deptno, count(*) as c from emp group by deptno;
                +--------+---+
                | DEPTNO | C |
                +--------+---+
                |     10 | 2 |
                |     20 | 1 |
                |     30 | 2 |
                |     50 | 2 |
                |     60 | 1 |
                |        | 1 |
                +--------+---+
                (6 rows)

                select distinct deptno from emp group by deptno;
                +--------+
                | DEPTNO |
                +--------+
                |     10 |
                |     20 |
                |     30 |
                |     50 |
                |     60 |
                |        |
                +--------+
                (6 rows)

                select distinct count(*) as c from emp group by deptno;
                +---+
                | C |
                +---+
                | 1 |
                | 2 |
                +---+
                (2 rows)

                select distinct count(*) as c from emp group by deptno having count(*) > 1;
                +---+
                | C |
                +---+
                | 2 |
                +---+
                (1 row)

                select distinct count(*) as c from emp group by deptno order by 1 desc
                ;
                +---+
                | C |
                +---+
                | 2 |
                | 1 |
                +---+
                (2 rows)

                -- [CALCITE-2192] RelBuilder wrongly skips creation of Aggregate that prunes
                -- columns if input is unique
                select distinct deptno
                from (select deptno, count(*) from emp group by deptno);
                +--------+
                | DEPTNO |
                +--------+
                |     10 |
                |     20 |
                |     30 |
                |     50 |
                |     60 |
                |        |
                +--------+
                (6 rows)""");
    }

    @Test
    public void issue1901() {
        this.qs("""
                select stddev(value) FROM (VALUES (.1e0), (.1e0), (.1e0)) t1(value) WHERE true;
                result
                ------
                 0e0
                (1 row)""");
    }

    @Test
    public void stddevTests() {
        this.qs("""
                -- Our own test, for denominator of 0 in SAMP
                select stddev_samp(deptno) as s from emp WHERE deptno = 60;
                +----+
                | S  |
                +----+
                |    |
                +----+
                (1 row)

                -- [CALCITE-998] Exception when calling STDDEV_SAMP, STDDEV_POP
                -- stddev_samp
                select stddev_samp(deptno) as s from emp;
                +----+
                | S  |
                +----+
                | 19 |
                +----+
                (1 row)

                -- [CALCITE-998] Exception when calling STDDEV_SAMP, STDDEV_POP
                -- stddev_samp
                select stddev_samp(deptno) as s from emp;
                +----+
                | S  |
                +----+
                | 19 |
                +----+
                (1 row)

                -- stddev_pop
                select stddev_pop(deptno) as s from emp;
                +----+
                | S  |
                +----+
                | 17 |
                +----+
                (1 row)

                -- stddev
                select stddev(deptno) as s from emp;
                +----+
                | S  |
                +----+
                | 19 |
                +----+
                (1 row)

                -- both
                select gender,
                  stddev_pop(deptno) as p,
                  stddev_samp(deptno) as s,
                  stddev(deptno) as ss,
                  count(deptno) as c
                from emp
                group by gender;
                +--------+----+----+----+---+
                | GENDER | P  | S  | SS | C |
                +--------+----+----+----+---+
                | F|       17 | 19 | 19 | 5 |
                | M|       17 | 20 | 20 | 3 |
                +--------+----+----+----+---+
                (2 rows)""");
    }

    @Test
    @Ignore("multiset not yet implemented")
    public void testIntersection() {
        this.qs("""
                -- [CALCITE-3815] Add missing SQL standard aggregate
                -- functions: EVERY, SOME, INTERSECTION
                select some(deptno = 100), every(deptno > 0), intersection(multiset[1, 2]) from emp;
                +--------+--------+--------+
                | EXPR$0 | EXPR$1 | EXPR$2 |
                +--------+--------+--------+
                | false  | true   | [1, 2] |
                +--------+--------+--------+
                (1 row)""");
    }

    @Test
    public void testEvery() {
        this.qs("""
                select some(deptno > 100), every(deptno > 0) from emp where deptno > 1000;
                +--------+--------+
                | EXPR$0 | EXPR$1 |
                +--------+--------+
                |        |        |
                +--------+--------+
                (1 row)
                """);
    }

    @Test
    public void simpleTests() {
        this.qs("""
                select city, gender as c from emps;
                +---------------+---+
                | CITY          | C |
                +---------------+---+
                | Vancouver| F|
                | San Francisco| M|
                |NULL|NULL|
                | Vancouver| M|
                |NULL| F|
                +---------------+---+
                (5 rows)

                -- SELECT DISTINCT includes fully and partially null rows
                select distinct city, gender from emps;
                +---------------+--------+
                | CITY          | GENDER |
                +---------------+--------+
                |NULL|NULL|
                | Vancouver| M|
                |NULL| F|
                | San Francisco| M|
                | Vancouver| F|
                +---------------+--------+
                (5 rows)

                -- Nulls in GROUP BY
                select x = 1 as x1, count(*) as c
                from (values 0, 1, 2, cast(null as integer)) as t(x)
                group by x = 1;
                X1 BOOLEAN(1)
                C BIGINT(19) NOT NULL
                !type
                +-------+---+
                | X1    | C |
                +-------+---+
                | false | 2 |
                | true  | 1 |
                |       | 1 |
                +-------+---+
                (3 rows)""");
    }

    @Test
    public void testGroupingSets() {
        this.qs("""
                -- Basic GROUPING SETS
                select deptno, count(*) as c from emps group by grouping sets ((), (deptno));
                +--------+---+
                | DEPTNO | C |
                +--------+---+
                |     10 | 1 |
                |     20 | 2 |
                |     40 | 2 |
                |        | 5 |
                +--------+---+
                (4 rows)

                -- GROUPING SETS on expression
                select deptno + 1, count(*) as c from emps group by grouping sets ((), (deptno + 1));
                +--------+---+
                | EXPR$0 | C |
                +--------+---+
                |     11 | 1 |
                |     21 | 2 |
                |     41 | 2 |
                |        | 5 |
                +--------+---+
                (4 rows)

                -- GROUPING SETS on single-row relation returns multiple rows
                select 1 as c
                from (values ('a', 'b')) as t (a, b)
                group by grouping sets ((a), (b), (b, a));
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
    public void testRollup() {
        this.qs("""
                -- CUBE
                select deptno + 1, count(*) as c from emp group by cube(deptno, gender);
                +--------+---+
                | EXPR$0 | C |
                +--------+---+
                |     11 | 1 |
                |     11 | 1 |
                |     11 | 2 |
                |     21 | 1 |
                |     21 | 1 |
                |     31 | 2 |
                |     31 | 2 |
                |     51 | 1 |
                |     51 | 1 |
                |     51 | 2 |
                |     61 | 1 |
                |     61 | 1 |
                |        | 1 |
                |        | 1 |
                |        | 3 |
                |        | 6 |
                |        | 9 |
                +--------+---+
                (17 rows)

                -- ROLLUP on 1 column
                select deptno + 1, count(*) as c
                from emp
                group by rollup(deptno);
                +--------+---+
                | EXPR$0 | C |
                +--------+---+
                |     11 | 2 |
                |     21 | 1 |
                |     31 | 2 |
                |     51 | 2 |
                |     61 | 1 |
                |        | 1 |
                |        | 9 |
                +--------+---+
                (7 rows)

                -- ROLLUP on 2 columns; project columns in different order
                select gender, deptno + 1, count(*) as c
                from emp
                group by rollup(deptno, gender);
                +--------+--------+---+
                | GENDER | EXPR$1 | C |
                +--------+--------+---+
                | M|           21 | 1 |
                | F|           11 | 1 |
                | F|           31 | 2 |
                | F|           51 | 1 |
                | F|           61 | 1 |
                | F|              | 1 |
                | M|           11 | 1 |
                | M|           51 | 1 |
                |NULL|         11 | 2 |
                |NULL|         21 | 1 |
                |NULL|         31 | 2 |
                |NULL|         51 | 2 |
                |NULL|         61 | 1 |
                |NULL|            | 1 |
                |NULL|            | 9 |
                +--------+--------+---+
                (15 rows)

                -- ROLLUP on column with nulls
                -- Note the two rows with NULL key (one represents ALL)
                select gender, count(*) as c
                from emp
                group by rollup(gender);
                +--------+---+
                | GENDER | C |
                +--------+---+
                | F|       6 |
                | M|       3 |
                |NULL|     9 |
                +--------+---+
                (3 rows)

                -- ROLLUP plus ORDER BY
                select gender, count(*) as c
                from emp
                group by rollup(gender)
                order by c desc
                ;
                +--------+---+
                | GENDER | C |
                +--------+---+
                |NULL|     9 |
                | F|       6 |
                | M|       3 |
                +--------+---+
                (3 rows)

                -- ROLLUP cartesian product
                select deptno, count(*) as c
                from emp
                group by rollup(deptno), rollup(gender);
                +--------+---+
                | DEPTNO | C |
                +--------+---+
                |     10 | 1 |
                |     10 | 1 |
                |     20 | 1 |
                |     20 | 1 |
                |        | 1 |
                |     10 | 2 |
                |     30 | 2 |
                |     30 | 2 |
                |     50 | 1 |
                |     50 | 1 |
                |     50 | 2 |
                |     60 | 1 |
                |     60 | 1 |
                |        | 1 |
                |        | 3 |
                |        | 6 |
                |        | 9 |
                +--------+---+
                (17 rows)

                -- ROLLUP cartesian product of with tuple with expression
                select deptno / 2 + 1 as half1, count(*) as c
                from emp
                group by rollup(deptno / 2, gender), rollup(substring(ename FROM 1 FOR 1));
                +-------+---+
                | HALF1 | C |
                +-------+---+
                |    11 | 1 |
                |    11 | 1 |
                |    11 | 1 |
                |    11 | 1 |
                |    16 | 1 |
                |    16 | 1 |
                |    16 | 1 |
                |    16 | 1 |
                |    16 | 2 |
                |    16 | 2 |
                |    26 | 1 |
                |    26 | 1 |
                |    26 | 1 |
                |    26 | 1 |
                |    26 | 1 |
                |    26 | 1 |
                |    26 | 2 |
                |    31 | 1 |
                |    31 | 1 |
                |    31 | 1 |
                |    31 | 1 |
                |     6 | 1 |
                |     6 | 1 |
                |     6 | 1 |
                |     6 | 1 |
                |     6 | 1 |
                |     6 | 1 |
                |     6 | 2 |
                |       | 1 |
                |       | 1 |
                |       | 1 |
                |       | 1 |
                |       | 1 |
                |       | 1 |
                |       | 1 |
                |       | 1 |
                |       | 1 |
                |       | 2 |
                |       | 2 |
                |       | 9 |
                +-------+---+
                (40 rows)

                -- ROLLUP with HAVING
                select deptno + 1 as d1, count(*) as c
                from emp
                group by rollup(deptno)
                having count(*) > 3;
                +----+---+
                | D1 | C |
                +----+---+
                |    | 9 |
                +----+---+
                (1 row)

                -- ROLLUP column used in expression; see [CALCITE-5296]
                -- In a query with ROLLUP, validator wrongly infers that a column is NOT NULL
                select deptno, deptno + 1 as d1 from emp group by rollup(deptno);
                +--------+----+
                | DEPTNO | D1 |
                +--------+----+
                |     10 | 11 |
                |     20 | 21 |
                |     30 | 31 |
                |     50 | 51 |
                |     60 | 61 |
                |        |    |
                |        |    |
                +--------+----+
                (7 rows)

                -- CUBE and DISTINCT
                select distinct count(*) from emp group by cube(deptno, gender);
                +--------+
                | EXPR$0 |
                +--------+
                |      1 |
                |      2 |
                |      3 |
                |      6 |
                |      9 |
                +--------+
                (5 rows)

                -- CUBE and ROLLUP cartesian product over same columns
                select deptno, gender, count(*) from emp where deptno = 20 group by cube(deptno, gender), rollup(deptno, gender);
                +--------+--------+--------+
                | DEPTNO | GENDER | EXPR$2 |
                +--------+--------+--------+
                |     20 | M|            1 |
                |     20 | M|            1 |
                |     20 | M|            1 |
                |     20 | M|            1 |
                |     20 | M|            1 |
                |     20 | M|            1 |
                |     20 | M|            1 |
                |     20 |NULL|          1 |
                |     20 |NULL|          1 |
                |     20 |NULL|          1 |
                |        | M|            1 |
                |        |NULL|          1 |
                +--------+--------+--------+
                (12 rows)

                -- GROUP BY DISTINCT CUBE and ROLLUP cartesian product over same columns
                select deptno, gender, count(*) from emp where deptno = 20 group by distinct cube(deptno, gender), rollup(deptno, gender);
                +--------+--------+--------+
                | DEPTNO | GENDER | EXPR$2 |
                +--------+--------+--------+
                |     20 | M|            1 |
                |     20 |NULL|          1 |
                |        | M|            1 |
                |        |NULL|          1 |
                +--------+--------+--------+
                (4 rows)""");
    }

    @Test
    public void testCornerCases() {
        this.qs("""
                -- GROUP BY over empty columns
                select count(*) from emp where deptno = 20 group by ();
                +--------+
                | EXPR$0 |
                +--------+
                |      1 |
                +--------+
                (1 row)

                -- GROUP BY DISTINCT over empty columns
                select count(*) from emp where deptno = 20 group by distinct ();
                +--------+
                | EXPR$0 |
                +--------+
                |      1 |
                +--------+
                (1 row)

                -- GROUP BY DISTINCT x + y
                select deptno + 1, count(*) from emp where deptno = 20 group by distinct deptno + 1;
                +--------+--------+
                | EXPR$0 | EXPR$1 |
                +--------+--------+
                |     21 |      1 |
                +--------+--------+
                (1 row)""");
    }

    @Test
    public void testGrouping() {
        this.qs("""
                -- CUBE and JOIN
                select e.deptno, e.gender, min(e.ename) as min_name
                from emp as e join dept as d using (deptno)
                group by cube(e.deptno, d.deptno, e.gender)
                having count(*) > 2 or gender = 'M' and e.deptno = 10;
                +--------+--------+----------+
                | DEPTNO | GENDER | MIN_NAME |
                +--------+--------+----------+
                |     10 | M| Bob|
                |     10 | M| Bob|
                |        | F| Alice|
                |        |NULL| Alice|
                +--------+--------+----------+
                (4 rows)

                -- GROUPING in SELECT clause of GROUP BY query
                select count(*) as c, grouping(deptno) as g
                from emp
                group by deptno;
                +---+---+
                | C | G |
                +---+---+
                | 1 | 0 |
                | 1 | 0 |
                | 1 | 0 |
                | 2 | 0 |
                | 2 | 0 |
                | 2 | 0 |
                +---+---+
                (6 rows)

                -- GROUPING, GROUP_ID, GROUPING_ID in SELECT clause of GROUP BY query
                select count(*) as c,
                  grouping(deptno) as g,
                  group_id() as gid,
                  grouping_id(deptno) as gd,
                  grouping_id(gender) as gg,
                  grouping_id(gender, deptno) as ggd,
                  grouping_id(deptno, gender) as gdg
                from emp
                group by rollup(deptno, gender);
                +---+---+-----+----+----+-----+-----+
                | C | G | GID | GD | GG | GGD | GDG |
                +---+---+-----+----+----+-----+-----+
                | 1 | 0 |   0 |  0 |  0 |   0 |   0 |
                | 1 | 0 |   0 |  0 |  0 |   0 |   0 |
                | 1 | 0 |   0 |  0 |  0 |   0 |   0 |
                | 1 | 0 |   0 |  0 |  0 |   0 |   0 |
                | 1 | 0 |   0 |  0 |  0 |   0 |   0 |
                | 1 | 0 |   0 |  0 |  0 |   0 |   0 |
                | 1 | 0 |   0 |  0 |  0 |   0 |   0 |
                | 2 | 0 |   0 |  0 |  0 |   0 |   0 |
                | 9 | 1 |   0 |  1 |  1 |   3 |   3 |
                | 1 | 0 |   0 |  0 |  1 |   2 |   1 |
                | 1 | 0 |   0 |  0 |  1 |   2 |   1 |
                | 1 | 0 |   0 |  0 |  1 |   2 |   1 |
                | 2 | 0 |   0 |  0 |  1 |   2 |   1 |
                | 2 | 0 |   0 |  0 |  1 |   2 |   1 |
                | 2 | 0 |   0 |  0 |  1 |   2 |   1 |
                +---+---+-----+----+----+-----+-----+
                (15 rows)

                -- GROUPING accepts multiple arguments, gives same result as GROUPING_ID
                select count(*) as c,
                  grouping(deptno) as gd,
                  grouping_id(deptno) as gid,
                  grouping(deptno, gender, deptno) as gdgd,
                  grouping_id(deptno, gender, deptno) as gidgd
                from emp
                group by rollup(deptno, gender)
                having grouping(deptno) <= grouping_id(deptno, gender, deptno);
                +---+----+-----+------+-------+
                | C | GD | GID | GDGD | GIDGD |
                +---+----+-----+------+-------+
                | 1 |  0 |   0 |    0 |     0 |
                | 1 |  0 |   0 |    0 |     0 |
                | 1 |  0 |   0 |    0 |     0 |
                | 1 |  0 |   0 |    0 |     0 |
                | 1 |  0 |   0 |    0 |     0 |
                | 1 |  0 |   0 |    0 |     0 |
                | 1 |  0 |   0 |    0 |     0 |
                | 2 |  0 |   0 |    0 |     0 |
                | 1 |  0 |   0 |    2 |     2 |
                | 1 |  0 |   0 |    2 |     2 |
                | 1 |  0 |   0 |    2 |     2 |
                | 2 |  0 |   0 |    2 |     2 |
                | 2 |  0 |   0 |    2 |     2 |
                | 2 |  0 |   0 |    2 |     2 |
                | 9 |  1 |   1 |    7 |     7 |
                +---+----+-----+------+-------+
                (15 rows)

                -- GROUPING in ORDER BY clause
                select count(*) as c
                from emp
                group by rollup(deptno)
                order by grouping(deptno), c;
                +---+
                | C |
                +---+
                | 1 |
                | 1 |
                | 1 |
                | 2 |
                | 2 |
                | 2 |
                | 9 |
                +---+
                (7 rows)

                -- Duplicate argument to GROUPING_ID.
                select deptno, gender, grouping_id(deptno, gender, deptno), count(*) as c
                from emp
                where deptno = 10
                group by rollup(gender, deptno);
                +--------+--------+--------+---+
                | DEPTNO | GENDER | EXPR$2 | C |
                +--------+--------+--------+---+
                |     10 | F|      0 | 1 |
                |     10 | M|      0 | 1 |
                |        | F|      5 | 1 |
                |        | M|      5 | 1 |
                |        |NULL|      7 | 2 |
                +--------+--------+--------+---+
                (5 rows)

                -- GROUPING in SELECT clause of ROLLUP query
                select count(*) as c, deptno, grouping(deptno) as g
                from emp
                group by rollup(deptno);
                +---+--------+---+
                | C | DEPTNO | G |
                +---+--------+---+
                | 1 |     20 | 0 |
                | 1 |     60 | 0 |
                | 1 |        | 0 |
                | 2 |     10 | 0 |
                | 2 |     30 | 0 |
                | 2 |     50 | 0 |
                | 9 |        | 1 |
                +---+--------+---+
                (7 rows)

                -- GROUPING, GROUPING_ID and GROUP_ID
                select deptno, gender, grouping(deptno) gd, grouping(gender) gg,
                  grouping_id(deptno, gender) dg, grouping_id(gender, deptno) gd,
                  group_id() gid, count(*) c
                from emp
                group by cube(deptno, gender);
                +--------+--------+----+----+----+----+-----+---+
                | DEPTNO | GENDER | GD | GG | DG | GD | GID | C |
                +--------+--------+----+----+----+----+-----+---+
                |     10 | F|        0 |  0 |  0 |  0 |   0 | 1 |
                |     10 | M|        0 |  0 |  0 |  0 |   0 | 1 |
                |     20 | M|        0 |  0 |  0 |  0 |   0 | 1 |
                |     30 | F|        0 |  0 |  0 |  0 |   0 | 2 |
                |     50 | F|        0 |  0 |  0 |  0 |   0 | 1 |
                |     50 | M|        0 |  0 |  0 |  0 |   0 | 1 |
                |     60 | F|        0 |  0 |  0 |  0 |   0 | 1 |
                |        | F|        0 |  0 |  0 |  0 |   0 | 1 |
                |        |NULL|      1 |  1 |  3 |  3 |   0 | 9 |
                |     10 |NULL|      0 |  1 |  1 |  2 |   0 | 2 |
                |     20 |NULL|      0 |  1 |  1 |  2 |   0 | 1 |
                |     30 |NULL|      0 |  1 |  1 |  2 |   0 | 2 |
                |     50 |NULL|      0 |  1 |  1 |  2 |   0 | 2 |
                |     60 |NULL|      0 |  1 |  1 |  2 |   0 | 1 |
                |        | F|        1 |  0 |  2 |  1 |   0 | 6 |
                |        | M|        1 |  0 |  2 |  1 |   0 | 3 |
                |        |NULL|      0 |  1 |  1 |  2 |   0 | 1 |
                +--------+--------+----+----+----+----+-----+---+
                (17 rows)""");
    }

    @Test
    public void testAgg() {
        this.qs("""
                -- [CALCITE-1781] Allow expression in CUBE and ROLLUP
                select deptno + 1 as d1, deptno + 1 - 1 as d0, count(*) as c
                from emp
                group by rollup(deptno + 1);
                +----+----+---+
                | D1 | D0 | C |
                +----+----+---+
                | 11 | 10 | 2 |
                | 21 | 20 | 1 |
                | 31 | 30 | 2 |
                | 51 | 50 | 2 |
                | 61 | 60 | 1 |
                |    |    | 1 |
                |    |    | 9 |
                +----+----+---+
                (7 rows)

                select mod(deptno, 20) as d, count(*) as c, gender as g
                from emp
                group by cube(mod(deptno, 20), gender);
                +----+---+---+
                | D  | C | G |
                +----+---+---+
                |  0 | 1 | F|
                |  0 | 1 | M|
                |  0 | 2 |NULL|
                | 10 | 2 | M|
                | 10 | 4 | F|
                | 10 | 6 |NULL|
                |    | 1 | F|
                |    | 1 |NULL|
                |    | 3 | M|
                |    | 6 | F|
                |    | 9 |NULL|
                +----+---+---+
                (11 rows)

                select mod(deptno, 20) as d, count(*) as c, gender as g
                from emp
                group by rollup(mod(deptno, 20), gender);
                +----+---+---+
                | D  | C | G |
                +----+---+---+
                |  0 | 1 | F|
                |  0 | 1 | M|
                |  0 | 2 |NULL|
                | 10 | 2 | M|
                | 10 | 4 | F|
                | 10 | 6 |NULL|
                |    | 1 | F|
                |    | 1 |NULL|
                |    | 9 |NULL|
                +----+---+---+
                (9 rows)

                -- modified to disambiguate 1 into '1',
                -- since it was interpreted as a column index
                select count(*) as c
                from emp
                group by cube('1');
                +---+
                | C |
                +---+
                | 9 |
                | 9 |
                +---+
                (2 rows)

                -- modified to disambiguate 1 into '1',
                -- since it was interpreted as a column index
                select count(*) as c
                from emp
                group by rollup('1');
                +---+
                | C |
                +---+
                | 9 |
                | 9 |
                +---+
                (2 rows)""");
    }

    @Test public void t0() {
        this.qs("""
                select distinct '1'
                from (values (1,2),(3,4));
                +--------+
                | EXPR$0 |
                +--------+
                | 1|
                +--------+
                (1 row)

                select count(distinct '1')
                from (values (1,2),(3,4));
                +--------+
                | EXPR$0 |
                +--------+
                |      1 |
                +--------+
                (1 row)

                -- [CALCITE-1381] SqlCall.clone should retain function quantifier
                select nullif(count(distinct '1'),0)
                from (values (1,2),(3,4));
                +--------+
                | EXPR$0 |
                +--------+
                |      1 |
                +--------+
                (1 row)""");
    }

   /*
   TODO: setup more tests
   @Test
   public void testAggregates() {
       this.qs("""
                !use orinoco

                -- FLOOR to achieve a 2-hour window
                select floor(rowtime to hour) as rowtime, count(*) as c
                from Orders
                group by floor(rowtime to hour);
                +---------------------+---+
                | ROWTIME             | C |
                +---------------------+---+
                | 2015-02-15 10:00:00 | 4 |
                | 2015-02-15 11:00:00 | 1 |
                +---------------------+---+
                (2 rows)

                -- FLOOR applied to intervals, to achieve a 2-hour window
                select rowtime, count(*) as c
                from (
                  select timestamp '1970-1-1 0:0:0' + (floor(timestamp '1970-1-1 0:0:0' + ((rowtime - timestamp '1970-1-1 0:0:0') second) / 2 to hour) - timestamp '1970-1-1 0:0:0') second * 2 as rowtime
                  from Orders)
                group by rowtime;
                +---------------------+---+
                | ROWTIME             | C |
                +---------------------+---+
                | 2015-02-15 10:00:00 | 5 |
                +---------------------+---+
                (1 row)

                !use scott

                -- [CALCITE-1776, CALCITE-2402] REGR_COUNT with group by
                SELECT SAL, regr_count(COMM, SAL) as "REGR_COUNT(COMM, SAL)",
                   regr_count(EMPNO, SAL) as "REGR_COUNT(EMPNO, SAL)"
                from "scott".emp group by SAL;
                +---------+-----------------------+------------------------+
                | SAL     | REGR_COUNT(COMM, SAL) | REGR_COUNT(EMPNO, SAL) |
                +---------+-----------------------+------------------------+
                | 1100.00 |                     0 |                      1 |
                | 1250.00 |                     2 |                      2 |
                | 1300.00 |                     0 |                      1 |
                | 1500.00 |                     1 |                      1 |
                | 1600.00 |                     1 |                      1 |
                | 2450.00 |                     0 |                      1 |
                | 2850.00 |                     0 |                      1 |
                | 2975.00 |                     0 |                      1 |
                | 3000.00 |                     0 |                      2 |
                | 5000.00 |                     0 |                      1 |
                |  800.00 |                     0 |                      1 |
                |  950.00 |                     0 |                      1 |
                +---------+-----------------------+------------------------+
                (12 rows)

                -- [CALCITE-1776, CALCITE-2402] COVAR_POP, COVAR_SAMP, VAR_SAMP, VAR_POP with group by
                SELECT
                  MONTH(HIREDATE) as "MONTH",
                  covar_samp(SAL, COMM) as "COVAR_SAMP(SAL, COMM)",
                  var_pop(COMM) as "VAR_POP(COMM)",
                  var_samp(SAL) as "VAR_SAMP(SAL)"
                from "scott".emp
                group by MONTH(HIREDATE);
                +-------+-----------------------+---------------+-------------------+
                | MONTH | COVAR_SAMP(SAL, COMM) | VAR_POP(COMM) | VAR_SAMP(SAL)     |
                +-------+-----------------------+---------------+-------------------+
                |     1 |                       |               |      1201250.0000 |
                |    11 |                       |               |                   |
                |    12 |                       |               | 1510833.333333334 |
                |     2 |           -35000.0000 |    10000.0000 |  831458.333333335 |
                |     4 |                       |               |                   |
                |     5 |                       |               |                   |
                |     6 |                       |               |                   |
                |     9 |          -175000.0000 |   490000.0000 |        31250.0000 |
                +-------+-----------------------+---------------+-------------------+
                (8 rows)

                -- [CALCITE-2224] WITHIN GROUP clause for aggregate functions
                select deptno, collect(empno) within group (order by empno asc) as empnos
                from "scott".emp
                group by deptno;
                +--------+--------------------------------------+
                | DEPTNO | EMPNOS                               |
                +--------+--------------------------------------+
                |     10 | [7782, 7839, 7934]                   |
                |     20 | [7369, 7566, 7788, 7876, 7902]       |
                |     30 | [7499, 7521, 7654, 7698, 7844, 7900] |
                +--------+--------------------------------------+
                (3 rows)

                select deptno, collect(empno) within group (order by empno desc) as empnos
                from "scott".emp
                group by deptno;
                +--------+--------------------------------------+
                | DEPTNO | EMPNOS                               |
                +--------+--------------------------------------+
                |     10 | [7934, 7839, 7782]                   |
                |     20 | [7902, 7876, 7788, 7566, 7369]       |
                |     30 | [7900, 7844, 7698, 7654, 7521, 7499] |
                +--------+--------------------------------------+
                (3 rows)

                select
                deptno,
                collect(empno) as empnos_1,
                collect(empno) within group (order by empno desc) as empnos_2
                from "scott".emp
                group by deptno;
                +--------+--------------------------------------+--------------------------------------+
                | DEPTNO | EMPNOS_1                             | EMPNOS_2                             |
                +--------+--------------------------------------+--------------------------------------+
                |     10 | [7782, 7839, 7934]                   | [7934, 7839, 7782]                   |
                |     20 | [7369, 7566, 7788, 7876, 7902]       | [7902, 7876, 7788, 7566, 7369]       |
                |     30 | [7499, 7521, 7654, 7698, 7844, 7900] | [7900, 7844, 7698, 7654, 7521, 7499] |
                +--------+--------------------------------------+--------------------------------------+
                (3 rows)

                select deptno, collect(empno) within group (order by empno desc)
                filter (where empno > 7500) as empnos
                from "scott".emp
                group by deptno;
                +--------+--------------------------------+
                | DEPTNO | EMPNOS                         |
                +--------+--------------------------------+
                |     10 | [7934, 7839, 7782]             |
                |     20 | [7902, 7876, 7788, 7566]       |
                |     30 | [7900, 7844, 7698, 7654, 7521] |
                +--------+--------------------------------+
                (3 rows)

                select deptno, collect(empno) within group (order by empno desc) as empnos1,
                collect(empno) within group (order by empno asc) as empnos2
                from "scott".emp
                group by deptno;
                +--------+--------------------------------------+--------------------------------------+
                | DEPTNO | EMPNOS1                              | EMPNOS2                              |
                +--------+--------------------------------------+--------------------------------------+
                |     10 | [7934, 7839, 7782]                   | [7782, 7839, 7934]                   |
                |     20 | [7902, 7876, 7788, 7566, 7369]       | [7369, 7566, 7788, 7876, 7902]       |
                |     30 | [7900, 7844, 7698, 7654, 7521, 7499] | [7499, 7521, 7654, 7698, 7844, 7900] |
                +--------+--------------------------------------+--------------------------------------+
                (3 rows)

                -- Aggregate WITHIN GROUP with JOIN
                select dept.deptno,
                  collect(sal) within group (order by sal desc) as s,
                  collect(sal) within group (order by 1)as s1,
                  collect(sal) within group (order by sal) filter (where sal > 2000) as s2
                from "scott".emp
                join "scott".dept using (deptno)
                group by dept.deptno;
                +--------+-------------------------------------------------------+-------------------------------------------------------+-----------------------------+
                | DEPTNO | S                                                     | S1                                                    | S2                          |
                +--------+-------------------------------------------------------+-------------------------------------------------------+-----------------------------+
                |     10 | [5000.00, 2450.00, 1300.00]                           | [2450.00, 5000.00, 1300.00]                           | [2450.00, 5000.00]          |
                |     20 | [3000.00, 3000.00, 2975.00, 1100.00, 800.00]          | [800.00, 2975.00, 3000.00, 1100.00, 3000.00]          | [2975.00, 3000.00, 3000.00] |
                |     30 | [2850.00, 1600.00, 1500.00, 1250.00, 1250.00, 950.00] | [1600.00, 1250.00, 1250.00, 2850.00, 1500.00, 950.00] | [2850.00]                   |
                +--------+-------------------------------------------------------+-------------------------------------------------------+-----------------------------+
                (3 rows)

                select deptno, collect(empno + 1) within group (order by 1) as empnos
                from "scott".emp
                group by deptno;
                +--------+--------------------------------------+
                | DEPTNO | EMPNOS                               |
                +--------+--------------------------------------+
                |     10 | [7783, 7840, 7935]                   |
                |     20 | [7370, 7567, 7789, 7877, 7903]       |
                |     30 | [7500, 7522, 7655, 7699, 7845, 7901] |
                +--------+--------------------------------------+
                (3 rows)

                -- Based on [DRUID-7593] Exact distinct-COUNT with complex expression (CASE, IN) throws
                -- NullPointerException
                WITH wikipedia AS (
                  SELECT empno AS delta,
                    CASE WHEN deptno = 10 THEN 'true' ELSE 'false' END AS isRobot,
                    ename AS "user"
                  FROM "scott".emp)
                SELECT COUNT(DISTINCT
                    CASE WHEN (((CASE WHEN wikipedia.delta IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                                      THEN REPLACE('Yes', 'Yes', 'Yes')
                                      ELSE REPLACE('No', 'No', 'No') END) = 'No'))
                         AND (wikipedia.isRobot = 'true')
                         THEN (wikipedia."user")
                         ELSE NULL END)
                  - (MAX(CASE WHEN (((CASE WHEN wikipedia.delta IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                                           THEN REPLACE('Yes', 'Yes', 'Yes')
                                           ELSE REPLACE('No', 'No', 'No') END) = 'No'))
                              AND (wikipedia.isRobot = 'true')
                              THEN NULL
                              ELSE -9223372036854775807 END)
                       + 9223372036854775807 + 1) AS "wikipedia.count_distinct_filters_that_dont_work"
                FROM wikipedia
                LIMIT 500;
                +-------------------------------------------------+
                | wikipedia.count_distinct_filters_that_dont_work |
                +-------------------------------------------------+
                |                                               2 |
                +-------------------------------------------------+
                (1 row)

                select listagg(ename) as combined_name from emp;
                +------------------------------------------------+
                | COMBINED_NAME                                  |
                +------------------------------------------------+
                | Jane,Bob,Eric,Susan,Alice,Adam,Eve,Grace,Wilma |
                +------------------------------------------------+
                (1 row)

                select listagg(ename) within group(order by gender, ename) as combined_name from emp;
                +------------------------------------------------+
                | COMBINED_NAME                                  |
                +------------------------------------------------+
                | Alice,Eve,Grace,Jane,Susan,Wilma,Adam,Bob,Eric |
                +------------------------------------------------+
                (1 row)

                select
                  listagg(ename) within group(order by deptno, ename) as default_listagg_sep,
                  listagg(ename, '; ') within group(order by deptno, ename desc) as custom_listagg_sep
                from emp group by gender;
                +----------------------------------+---------------------------------------+
                | DEFAULT_LISTAGG_SEP              | CUSTOM_LISTAGG_SEP                    |
                +----------------------------------+---------------------------------------+
                | Bob,Eric,Adam                    | Bob; Eric; Adam                       |
                | Jane,Alice,Susan,Eve,Grace,Wilma | Jane; Susan; Alice; Eve; Grace; Wilma |
                +----------------------------------+---------------------------------------+
                (2 rows)

                !use mysqlfunc

                -- GROUP_CONCAT (MySQL) is very similar to LISTAGG.

                -- GROUP_CONCAT with DISTINCT, SEPARATOR
                select
                    group_concat(distinct ename order by ename) as combined_name,
                    group_concat(ename order by ename separator ';') as separated_name
                from emp;
                +------------------------------------------------+------------------------------------------------+
                | COMBINED_NAME                                  | SEPARATED_NAME                                 |
                +------------------------------------------------+------------------------------------------------+
                | Adam,Alice,Bob,Eric,Eve,Grace,Jane,Susan,Wilma | Adam;Alice;Bob;Eric;Eve;Grace;Jane;Susan;Wilma |
                +------------------------------------------------+------------------------------------------------+
                (1 row)

                -- GROUP_CONCAT with multiple columns
                select
                    group_concat(deptno, ename order by ename) as combined_name
                from emp;
                +-----------------------------------------------+
                | COMBINED_NAME                                 |
                +-----------------------------------------------+
                | 50Alice30Bob10Eric20Eve50Grace60Jane10Susan30 |
                +-----------------------------------------------+
                (1 row)

                -- STRING_AGG (BigQuery and PostgreSQL) is very similar to LISTAGG.
                select
                  string_agg(ename order by deptno, ename) as default_string_agg_sep,
                  string_agg(ename, '; ' order by deptno, ename desc) as custom_string_agg_sep
                from emp group by gender;
                +----------------------------------+---------------------------------------+
                | DEFAULT_STRING_AGG_SEP           | CUSTOM_STRING_AGG_SEP                 |
                +----------------------------------+---------------------------------------+
                | Bob,Eric,Adam                    | Bob; Eric; Adam                       |
                | Jane,Alice,Susan,Eve,Grace,Wilma | Jane; Susan; Alice; Eve; Grace; Wilma |
                +----------------------------------+---------------------------------------+
                (2 rows)

                -- [CALCITE-3661] Add MODE aggregate function

                -- MODE without GROUP BY
                select MODE(gender) as m
                from emp;
                +---+
                | M |
                +---+
                | F |
                +---+
                (1 row)

                -- MODE with DISTINCT is pretty much useless (because every value occurs once),
                -- but we allow it. It returns the first value seen, in this case 'F'.
                select MODE(distinct gender) as m
                from emp;
                +---+
                | M |
                +---+
                | F |
                +---+
                (1 row)

                -- MODE function with WHERE.
                select MODE(gender) as m
                from emp
                where deptno <= 20;
                +---+
                | M |
                +---+
                | M |
                +---+
                (1 row)

                -- MODE function with WHERE that removes all rows.
                -- Result is NULL even though MODE is applied to a not-NULL column.
                select MODE(gender) as m
                from emp
                where deptno > 60;
                +---+
                | M |
                +---+
                |   |
                +---+
                (1 row)

                -- MODE function with GROUP BY.
                select deptno, MODE(gender) as m
                from emp
                where deptno > 10
                group by deptno;
                +--------+---+
                | DEPTNO | M |
                +--------+---+
                |     20 | M |
                |     30 | F |
                |     50 | M |
                |     60 | F |
                +--------+---+
                (4 rows)

                -- MODE function with GROUP BY; note that key is NULL but result is not NULL.
                select deptno, MODE(gender) as m
                from emp
                where ename = 'Wilma'
                group by deptno;
                +--------+---+
                | DEPTNO | M |
                +--------+---+
                |        | F |
                +--------+---+
                (1 row)

                -- MODE function with GROUP BY; key is NULL and input value is NULL.
                select deptno, MODE(deptno) as m
                from emp
                where ename = 'Wilma'
                group by deptno;
                +--------+---+
                | DEPTNO | M |
                +--------+---+
                |        |   |
                +--------+---+
                (1 row)

                -- MODE function applied to NULL value.
                -- (Calcite requires CAST so that it can deduce type.)
                select deptno, MODE(CAST(null AS INTEGER)) as m
                from emp
                group by deptno;
                +--------+---+
                | DEPTNO | M |
                +--------+---+
                |     10 |   |
                |     20 |   |
                |     30 |   |
                |     50 |   |
                |     60 |   |
                |        |   |
                +--------+---+
                (6 rows)

                -- MODE function with GROUPING SETS.
                select deptno, ename, MODE(gender) as m
                from emp
                group by grouping sets (deptno, ename);
                +--------+-------+---+
                | DEPTNO | ENAME | M |
                +--------+-------+---+
                |     10 |       | F |
                |     20 |       | M |
                |     30 |       | F |
                |     50 |       | M |
                |     60 |       | F |
                |        | Adam  | M |
                |        | Alice | F |
                |        | Bob   | M |
                |        | Eric  | M |
                |        | Eve   | F |
                |        | Grace | F |
                |        | Jane  | F |
                |        | Susan | F |
                |        | Wilma | F |
                |        |       | F |
                +--------+-------+---+
                (15 rows)

                -- Test case for [CALCITE-5388] tempList expression inside EnumerableWindow.getPartitionIterator should be unoptimized
                with
                    CTE1(rownr1, val1) as ( select ROW_NUMBER() OVER(ORDER BY id ASC), id from (values (1), (2)) as Vals1(id) ),
                    CTE2(rownr2, val2) as ( select ROW_NUMBER() OVER(ORDER BY id ASC), id from (values (1), (2)) as Vals2(id) )
                select
                    CTE1.rownr1, CTE1.val1, CTE2.rownr2, CTE2.val2
                from
                    CTE1,CTE2
                where
                    CTE1.val1 = CTE2.val2;
                +--------+------+--------+------+
                | ROWNR1 | VAL1 | ROWNR2 | VAL2 |
                +--------+------+--------+------+
                |      1 |    1 |      1 |    1 |
                |      2 |    2 |      2 |    2 |
                +--------+------+--------+------+
                (2 rows)
                */
}
