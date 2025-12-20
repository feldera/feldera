package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Test;

/** Tests from Calcite's sort.iq */
public class SortTests extends FoodmartBaseTests {
    @Test public void testSortOffset() {
        this.qs("""
                select *
                from dept
                order by deptno offset 1;
                +--------+------------+---------+
                | DEPTNO | DNAME      | LOC     |
                +--------+------------+---------+
                |     20 | RESEARCH| DALLAS|
                |     30 | SALES| CHICAGO|
                |     40 | OPERATIONS| BOSTON|
                +--------+------------+---------+
                (3 rows)
                
                select *
                from dept
                order by deptno limit 1, all;
                +--------+------------+---------+
                | DEPTNO | DNAME      | LOC     |
                +--------+------------+---------+
                |     20 | RESEARCH| DALLAS|
                |     30 | SALES| CHICAGO|
                |     40 | OPERATIONS| BOSTON|
                +--------+------------+---------+
                (3 rows)""");
    }

    @Test
    public void testSort() {
        this.qs("""
                select * from "days" order by "day";
                +-----+----------+
                | day | week_day |
                +-----+-------+
                |   1 | Sunday|
                |   2 | Monday|
                |   3 | Tuesday|
                |   4 | Wednesday|
                |   5 | Thursday|
                |   6 | Friday|
                |   7 | Saturday|
                +-----+-------+
                (7 rows)
                
                select * from "days" order by "day" limit 2;
                +-----+----------+
                | day | week_day |
                +-----+-------+
                |   1 | Sunday|
                |   2 | Monday|
                +-----+-------+
                (2 rows)
                
                select * from "days" where "day" between 2 and 4 order by "day";
                +-----+-----------+
                | day | week_day  |
                +-----+-----------+
                |   2 | Monday|
                |   3 | Tuesday|
                |   4 | Wednesday|
                +-----+-----------+
                (3 rows)
                
                select "store_id", "grocery_sqft" from "store"
                where "store_id" < 3
                order by 2 DESC;
                +----------+--------------+
                | store_id | grocery_sqft |
                +----------+--------------+
                |        0 |              |
                |        2 |        22271 |
                |        1 |        17475 |
                +----------+--------------+
                (3 rows)
                
                select "store_id", "grocery_sqft" from "store"
                where "store_id" < 3
                order by "florist", 2 DESC;
                +----------+--------------+
                | store_id | grocery_sqft |
                +----------+--------------+
                |        0 |              |
                |        2 |        22271 |
                |        1 |        17475 |
                +----------+--------------+
                (3 rows)
                
                select "store_id", "grocery_sqft" from "store"
                where "store_id" < 3
                order by 2;
                +----------+--------------+
                | store_id | grocery_sqft |
                +----------+--------------+
                |        1 |        17475 |
                |        2 |        22271 |
                |        0 |              |
                +----------+--------------+
                (3 rows)
                
                select "store_id", "grocery_sqft" from "store"
                where "store_id" < 3
                order by "florist", 2;
                +----------+--------------+
                | store_id | grocery_sqft |
                +----------+--------------+
                |        1 |        17475 |
                |        2 |        22271 |
                |        0 |              |
                +----------+--------------+
                (3 rows)
                
                select *
                from DEPT
                order by deptno desc, dname, deptno;
                +--------+------------+----------+
                | DEPTNO | DNAME      | LOC      |
                +--------+------------+----------+
                |     40 | OPERATIONS| BOSTON|
                |     30 | SALES| CHICAGO|
                |     20 | RESEARCH| DALLAS|
                |     10 | ACCOUNTING| NEW YORK|
                +--------+------------+----------+
                (4 rows)
                
                select *
                from dept
                order by deptno limit 2 offset 1;
                +--------+----------+---------+
                | DEPTNO | DNAME    | LOC     |
                +--------+----------+---------+
                |     20 | RESEARCH| DALLAS|
                |     30 | SALES| CHICAGO|
                +--------+----------+---------+
                (2 rows)
                
                select *
                from dept
                order by deptno offset 1 fetch next 2 rows only;
                +--------+----------+---------+
                | DEPTNO | DNAME    | LOC     |
                +--------+----------+---------+
                |     20 | RESEARCH| DALLAS|
                |     30 | SALES| CHICAGO|
                +--------+----------+---------+
                (2 rows)
                
                select *
                from dept
                order by deptno limit 2;
                +--------+------------+----------+
                | DEPTNO | DNAME      | LOC      |
                +--------+------------+----------+
                |     10 | ACCOUNTING| NEW YORK|
                |     20 | RESEARCH| DALLAS|
                +--------+------------+----------+
                (2 rows)
                
                select *
                from dept
                order by deptno fetch next 2 rows only;
                +--------+------------+----------+
                | DEPTNO | DNAME      | LOC      |
                +--------+------------+----------+
                |     10 | ACCOUNTING| NEW YORK|
                |     20 | RESEARCH| DALLAS|
                +--------+------------+----------+
                (2 rows)
                
                select *
                from dept
                order by deptno;
                +--------+------------+----------+
                | DEPTNO | DNAME      | LOC      |
                +--------+------------+----------+
                |     10 | ACCOUNTING| NEW YORK|
                |     20 | RESEARCH| DALLAS|
                |     30 | SALES| CHICAGO|
                |     40 | OPERATIONS| BOSTON|
                +--------+------------+----------+
                (4 rows)
                
                select *
                from dept
                order by deptno offset 1 limit 2;
                +--------+----------+---------+
                | DEPTNO | DNAME    | LOC     |
                +--------+----------+---------+
                |     20 | RESEARCH| DALLAS|
                |     30 | SALES| CHICAGO|
                +--------+----------+---------+
                (2 rows)
                
                select *
                from dept
                order by deptno limit 1, 2;
                +--------+----------+---------+
                | DEPTNO | DNAME    | LOC     |
                +--------+----------+---------+
                |     20 | RESEARCH| DALLAS|
                |     30 | SALES| CHICAGO|
                +--------+----------+---------+
                (2 rows)
                
                select *
                from dept
                order by deptno limit 2;
                +--------+------------+----------+
                | DEPTNO | DNAME      | LOC      |
                +--------+------------+----------+
                |     10 | ACCOUNTING| NEW YORK|
                |     20 | RESEARCH| DALLAS|
                +--------+------------+----------+
                (2 rows)
                
                select *
                from dept
                order by deptno limit all;
                +--------+------------+----------+
                | DEPTNO | DNAME      | LOC      |
                +--------+------------+----------+
                |     10 | ACCOUNTING| NEW YORK|
                |     20 | RESEARCH| DALLAS|
                |     30 | SALES| CHICAGO|
                |     40 | OPERATIONS| BOSTON|
                +--------+------------+----------+
                (4 rows)
                """);
    }
}
