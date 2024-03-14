package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Ignore;
import org.junit.Test;

// https://github.com/apache/calcite/blob/main/core/src/test/resources/sql/agg.iq
public class AggTests extends PostBaseTests {
    @Test @Ignore("https://github.com/feldera/feldera/issues/1481")
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
                | 10 |   |
                |  0 | M |
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

    @Test @Ignore("STDDEV not yet implemented")
    public void testStddev() {
        this.qs("""
                -- [CALCITE-998] Exception when calling STDDEV_SAMP, STDDEV_POP
                -- stddev_samp
                select stddev_samp(deptno) as s from emp;
                +----+
                | S  |
                +----+
                | 19 |
                +----+
                (1 row)
                                
                -- [CALCITE-3815] Add missing SQL standard aggregate
                -- functions: EVERY, SOME, INTERSECTION
                select some(deptno = 100), every(deptno > 0), intersection(multiset[1, 2]) from emp;
                +--------+--------+--------+
                | EXPR$0 | EXPR$1 | EXPR$2 |
                +--------+--------+--------+
                | false  | true   | [1, 2] |
                +--------+--------+--------+
                (1 row)
                                
                select some(deptno > 100), every(deptno > 0) from emp where deptno > 1000;
                +--------+--------+
                | EXPR$0 | EXPR$1 |
                +--------+--------+
                |        |        |
                +--------+--------+
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
                | F      | 17 | 19 | 19 | 5 |
                | M      | 17 | 20 | 20 | 3 |
                +--------+----+----+----+---+
                (2 rows)""");
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

    @Test public void testGrouping() {
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
                                
                select count(*) as c
                from emp
                group by cube(1);
                +---+
                | C |
                +---+
                | 9 |
                | 9 |
                +---+
                (2 rows)
                                
                select count(*) as c
                from emp
                group by rollup(1);
                +---+
                | C |
                +---+
                | 9 |
                | 9 |
                +---+
                (2 rows)""");
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

                -- [CALCITE-729] IndexOutOfBoundsException in ROLLUP query on JDBC data source
                !use jdbc_scott
                select deptno, job, count(*) as c
                from jdbc_scott.emp
                group by rollup (deptno, job)
                order by 1, 2;
                +--------+-----------+----+
                | DEPTNO | JOB       | C  |
                +--------+-----------+----+
                |     10 | CLERK     |  1 |
                |     10 | MANAGER   |  1 |
                |     10 | PRESIDENT |  1 |
                |     10 |           |  3 |
                |     20 | ANALYST   |  2 |
                |     20 | CLERK     |  2 |
                |     20 | MANAGER   |  1 |
                |     20 |           |  5 |
                |     30 | CLERK     |  1 |
                |     30 | MANAGER   |  1 |
                |     30 | SALESMAN  |  4 |
                |     30 |           |  6 |
                |        |           | 14 |
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
                (1 row)

                -- [CALCITE-886] System functions in the GROUP BY clause
                -- Calls to system functions do not have "()", which may confuse the validator.
                select CURRENT_USER as CUSER
                from jdbc_scott.emp
                group by CURRENT_USER;
                +-------+
                | CUSER |
                +-------+
                | SCOTT |
                +-------+
                (1 row)

                -- [CALCITE-886] System functions in the GROUP BY clause
                -- System function inside a GROUPING SETS.
                select CURRENT_USER as CUSER
                from jdbc_scott.emp
                group by grouping sets(CURRENT_USER);
                +-------+
                | CUSER |
                +-------+
                | SCOTT |
                +-------+
                (1 row)

                -- [CALCITE-1381] SqlCall.clone should retain function quantifier
                select nullif(count(distinct '1'),0)
                from (values (1,2),(3,4));
                +--------+
                | EXPR$0 |
                +--------+
                |      1 |
                +--------+
                (1 row)

                !use scott
                                
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
                    FROM "scott".emp
                    GROUP BY deptno, job) AS EmpAnalytics
                WHERE deptno = 30
                GROUP BY job;
                +----------+----------+
                | JOB      | AVG_SAL2 |
                +----------+----------+
                | CLERK    |   950.00 |
                | MANAGER  |  2850.00 |
                | SALESMAN |  1400.00 |
                +----------+----------+
                (3 rows)

                -- Same, using WITH
                WITH EmpAnalytics AS (
                    SELECT deptno, job, AVG(sal) AS avg_sal
                    FROM "scott".emp
                    GROUP BY deptno, job)
                SELECT job, AVG(avg_sal) AS avg_sal2
                FROM EmpAnalytics
                WHERE deptno = 30
                GROUP BY job;
                +----------+----------+
                | JOB      | AVG_SAL2 |
                +----------+----------+
                | CLERK    |   950.00 |
                | MANAGER  |  2850.00 |
                | SALESMAN |  1400.00 |
                +----------+----------+
                (3 rows)

                -- [CALCITE-1930] AggregateExpandDistinctAggregateRules should handle multiple aggregate calls with same input ref
                select count(distinct EMPNO), COUNT(SAL), MIN(SAL), MAX(SAL) from "scott".emp;
                +--------+--------+--------+---------+
                | EXPR$0 | EXPR$1 | EXPR$2 | EXPR$3  |
                +--------+--------+--------+---------+
                |     14 |     14 | 800.00 | 5000.00 |
                +--------+--------+--------+---------+
                (1 row)

                -- [CALCITE-1930] AggregateExpandDistinctAggregateRules should handle multiple aggregate calls with same input ref
                select count(distinct DEPTNO), COUNT(JOB), MIN(SAL), MAX(SAL) from "scott".emp;
                +--------+--------+--------+---------+
                | EXPR$0 | EXPR$1 | EXPR$2 | EXPR$3  |
                +--------+--------+--------+---------+
                |      3 |     14 | 800.00 | 5000.00 |
                +--------+--------+--------+---------+
                (1 row)

                -- [CALCITE-1930] AggregateExpandDistinctAggregateRules should handle multiple aggregate calls with same input ref
                select MGR, count(distinct DEPTNO), COUNT(JOB), MIN(SAL), MAX(SAL) from "scott".emp group by MGR;
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
                select MGR, count(distinct DEPTNO, JOB), MIN(SAL), MAX(SAL) from "scott".emp group by MGR;
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
                (7 rows)

                -- [CALCITE-2366] Add support for ANY_VALUE function
                -- Without GROUP BY clause
                SELECT any_value(empno) as anyempno from "scott".emp;
                +----------+
                | ANYEMPNO |
                +----------+
                |     7934 |
                +----------+
                (1 row)

                -- [CALCITE-2366] Add support for ANY_VALUE function
                -- With GROUP BY clause
                SELECT any_value(empno) as anyempno from "scott".emp group by sal;
                +----------+
                | ANYEMPNO |
                +----------+
                |     7369 |
                |     7499 |
                |     7566 |
                |     7654 |
                |     7698 |
                |     7782 |
                |     7839 |
                |     7844 |
                |     7876 |
                |     7900 |
                |     7902 |
                |     7934 |
                +----------+
                (12 rows)

                -- [CALCITE-1776, CALCITE-2402] REGR_COUNT
                SELECT regr_count(COMM, SAL) as "REGR_COUNT(COMM, SAL)",
                   regr_count(EMPNO, SAL) as "REGR_COUNT(EMPNO, SAL)"
                from "scott".emp;
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
                from "scott".emp;
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
                from "scott".emp;
                +-----------------------+----------------------+---------------+-------------------+
                | COVAR_POP(COMM, COMM) | COVAR_SAMP(SAL, SAL) | VAR_POP(COMM) | VAR_SAMP(SAL)     |
                +-----------------------+----------------------+---------------+-------------------+
                |           272500.0000 |    1398313.873626374 |   272500.0000 | 1398313.873626374 |
                +-----------------------+----------------------+---------------+-------------------+
                (1 row)
                                
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
                                
                -- BIT_AND, BIT_OR, BIT_XOR aggregate functions
                select bit_and(deptno), bit_or(deptno), bit_xor(deptno) from "scott".emp;
                +--------+--------+--------+
                | EXPR$0 | EXPR$1 | EXPR$2 |
                +--------+--------+--------+
                |      0 |     30 |     30 |
                +--------+--------+--------+
                (1 row)
                                
                select deptno, bit_and(empno), bit_or(empno), bit_xor(empno) from "scott".emp group by deptno;
                +--------+--------+--------+--------+
                | DEPTNO | EXPR$1 | EXPR$2 | EXPR$3 |
                +--------+--------+--------+--------+
                |     10 |   7686 |   7935 |   7687 |
                |     20 |   7168 |   8191 |   7985 |
                |     30 |   7168 |   8191 |    934 |
                +--------+--------+--------+--------+
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
                                
                -- [CALCITE-2266] JSON_OBJECTAGG, JSON_ARRAYAGG
                !use post
                                
                select gender, json_objectagg(ename: deptno absent on null) from emp group by gender;
                +--------+-------------------------------------------------------+
                | GENDER | EXPR$1                                                |
                +--------+-------------------------------------------------------+
                | F      | {"Eve":50,"Grace":60,"Susan":30,"Alice":30,"Jane":10} |
                | M      | {"Adam":50,"Bob":10,"Eric":20}                        |
                +--------+-------------------------------------------------------+
                (2 rows)

                select gender, json_arrayagg(json_object('ename': ename, 'deptno': deptno) format json) from emp group by gender;
                +--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | GENDER | EXPR$1                                                                                                                                                                               |
                +--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | F      | [{"ename":"Jane","deptno":10},{"ename":"Susan","deptno":30},{"ename":"Alice","deptno":30},{"ename":"Eve","deptno":50},{"ename":"Grace","deptno":60},{"ename":"Wilma","deptno":null}] |
                | M      | [{"ename":"Bob","deptno":10},{"ename":"Eric","deptno":20},{"ename":"Adam","deptno":50}]                                                                                              |
                +--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                (2 rows)

                select gender, json_arrayagg(json_object('ename': ename, 'deptno': deptno)) from emp group by gender;
                +--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | GENDER | EXPR$1                                                                                                                                                                               |
                +--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                | F      | [{"ename":"Jane","deptno":10},{"ename":"Susan","deptno":30},{"ename":"Alice","deptno":30},{"ename":"Eve","deptno":50},{"ename":"Grace","deptno":60},{"ename":"Wilma","deptno":null}] |
                | M      | [{"ename":"Bob","deptno":10},{"ename":"Eric","deptno":20},{"ename":"Adam","deptno":50}]                                                                                              |
                +--------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
                (2 rows)
                                
                select json_object('deptno': deptno, 'employees': json_arrayagg(json_object('ename': ename, 'gender': gender) format json) format json) from emp group by deptno;
                +-------------------------------------------------------------------------------------------+
                | EXPR$0                                                                                    |
                +-------------------------------------------------------------------------------------------+
                | {"employees":[{"ename":"Adam","gender":"M"},{"ename":"Eve","gender":"F"}],"deptno":50}    |
                | {"employees":[{"ename":"Eric","gender":"M"}],"deptno":20}                                 |
                | {"employees":[{"ename":"Grace","gender":"F"}],"deptno":60}                                |
                | {"employees":[{"ename":"Jane","gender":"F"},{"ename":"Bob","gender":"M"}],"deptno":10}    |
                | {"employees":[{"ename":"Susan","gender":"F"},{"ename":"Alice","gender":"F"}],"deptno":30} |
                | {"employees":[{"ename":"Wilma","gender":"F"}],"deptno":null}                              |
                +-------------------------------------------------------------------------------------------+
                (6 rows)

                select json_object('deptno': deptno, 'employees': json_arrayagg(json_object('ename': ename, 'gender': gender))) from emp group by deptno;
                +-------------------------------------------------------------------------------------------+
                | EXPR$0                                                                                    |
                +-------------------------------------------------------------------------------------------+
                | {"employees":[{"ename":"Adam","gender":"M"},{"ename":"Eve","gender":"F"}],"deptno":50}    |
                | {"employees":[{"ename":"Eric","gender":"M"}],"deptno":20}                                 |
                | {"employees":[{"ename":"Grace","gender":"F"}],"deptno":60}                                |
                | {"employees":[{"ename":"Jane","gender":"F"},{"ename":"Bob","gender":"M"}],"deptno":10}    |
                | {"employees":[{"ename":"Susan","gender":"F"},{"ename":"Alice","gender":"F"}],"deptno":30} |
                | {"employees":[{"ename":"Wilma","gender":"F"}],"deptno":null}                              |
                +-------------------------------------------------------------------------------------------+
                (6 rows)
                                
                -- [CALCITE-2786] Add order by clause support for JSON_ARRAYAGG
                select gender,
                json_arrayagg(deptno order by deptno),
                json_arrayagg(deptno order by deptno desc)
                from emp group by gender;
                +--------+------------------+------------------+
                | GENDER | EXPR$1           | EXPR$2           |
                +--------+------------------+------------------+
                | F      | [10,30,30,50,60] | [60,50,30,30,10] |
                | M      | [10,20,50]       | [50,20,10]       |
                +--------+------------------+------------------+
                (2 rows)

                -- [CALCITE-2787] Json aggregate calls with different null clause get incorrectly merged
                -- during converting from SQL to relational algebra
                select gender,
                json_arrayagg(deptno),
                json_arrayagg(deptno null on null)
                from emp group by gender;
                +--------+------------------+-----------------------+
                | GENDER | EXPR$1           | EXPR$2                |
                +--------+------------------+-----------------------+
                | F      | [10,30,30,50,60] | [10,30,30,50,60,null] |
                | M      | [10,20,50]       | [10,20,50]            |
                +--------+------------------+-----------------------+
                (2 rows)

                select gender,
                json_objectagg(ename: deptno),
                json_objectagg(ename: deptno absent on null)
                from emp group by gender;
                +--------+--------------------------------------------------------------------+-------------------------------------------------------+
                | GENDER | EXPR$1                                                             | EXPR$2                                                |
                +--------+--------------------------------------------------------------------+-------------------------------------------------------+
                | F      | {"Eve":50,"Grace":60,"Wilma":null,"Susan":30,"Alice":30,"Jane":10} | {"Eve":50,"Grace":60,"Susan":30,"Alice":30,"Jane":10} |
                | M      | {"Adam":50,"Bob":10,"Eric":20}                                     | {"Adam":50,"Bob":10,"Eric":20}                        |
                +--------+--------------------------------------------------------------------+-------------------------------------------------------+
                (2 rows)

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
                (1 row)

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
                | Adam  |     50 |        |   0 |   0 |   1 |
                | Adam  |        |        |   0 |   1 |   1 |
                | Bob   |     10 |        |   0 |   0 |   1 |
                | Bob   |        |        |   0 |   1 |   1 |
                | Eric  |     20 |        |   0 |   0 |   1 |
                | Eric  |        |        |   0 |   1 |   1 |
                |       |     10 |        |   1 |   0 |   1 |
                |       |     20 |        |   1 |   0 |   1 |
                |       |     50 |        |   1 |   0 |   1 |
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
                | Adam  |     50 |   0 |   0 |
                | Adam  |        |   0 |   1 |
                | Bob   |     10 |   0 |   0 |
                | Bob   |        |   0 |   1 |
                | Eric  |     20 |   0 |   0 |
                | Eric  |        |   0 |   1 |
                |       |     10 |   1 |   0 |
                |       |     20 |   1 |   0 |
                |       |     50 |   1 |   0 |
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
                                
                !set insubquerythreshold 5
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
                                
                -- [CALCITE-5283] Add ARG_MIN, ARG_MAX aggregate function
                                
                -- ARG_MIN, ARG_MAX without GROUP BY
                select arg_min(ename, deptno) as mi, arg_max(ename, deptno) as ma
                from emp;
                +-------+-------+
                | MI    | MA    |
                +-------+-------+
                | CLARK | ALLEN |
                +-------+-------+
                (1 row)
                                
                -- ARG_MIN, ARG_MAX with DISTINCT
                select arg_min(distinct ename, deptno) as mi, arg_max(distinct ename, deptno) as ma
                from emp;
                +-------+-------+
                | MI    | MA    |
                +-------+-------+
                | CLARK | ALLEN |
                +-------+-------+
                (1 row)
                                
                -- ARG_MIN, ARG_MAX function with WHERE.
                select arg_min(ename, deptno) as mi, arg_max(ename, deptno) as ma
                from emp
                where deptno <= 20;
                +-------+-------+
                | MI    | MA    |
                +-------+-------+
                | CLARK | SMITH |
                +-------+-------+
                (1 row)
                                
                -- ARG_MIN, ARG_MAX function with WHERE that removes all rows.
                -- Result is NULL even though ARG_MIN, ARG_MAX is applied to a not-NULL column.
                select arg_min(ename, deptno) as mi, arg_max(ename, deptno) as ma
                from emp
                where deptno > 60;
                +----+----+
                | MI | MA |
                +----+----+
                |    |    |
                +----+----+
                (1 row)
                                
                -- ARG_MIN, ARG_MAX function with GROUP BY. note that key is NULL but result is not NULL.
                select deptno, arg_min(ename, ename) as mi, arg_max(ename, ename) as ma
                from emp
                group by deptno;
                +--------+-------+--------+
                | DEPTNO | MI    | MA     |
                +--------+-------+--------+
                |     10 | CLARK | MILLER |
                |     20 | ADAMS | SMITH  |
                |     30 | ALLEN | WARD   |
                +--------+-------+--------+
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
     */
}
