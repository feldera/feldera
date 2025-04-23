package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Ignore;
import org.junit.Test;

// From from winagg.iq that use the HR database
public class HRWinAggTests extends HrBaseTests {
    @Test
    @Ignore("ORDER BY strings not supported https://github.com/feldera/feldera/issues/457, first_value")
    public void testWin() {
            this.qs("""
                -- [CALCITE-2081] Two windows under a JOIN
                select a.deptno, a.r as ar, b.r as br
                from (
                  select deptno, first_value(empid) over w as r
                  from emps
                  window w as (partition by deptno order by commission)) a
                join (
                  select deptno, last_value(empid) over w as r
                  from emps
                  window w as (partition by deptno order by commission)) b
                on a.deptno = b.deptno
                order by deptno, ar, br limit 5;
                +--------+-----+-----+
                | deptno | AR  | BR  |
                +--------+-----+-----+
                |     10 | 110 | 100 |
                |     10 | 110 | 100 |
                |     10 | 110 | 100 |
                |     10 | 110 | 110 |
                |     10 | 110 | 110 |
                +--------+-----+-----+
                (5 rows)

                select a."empid", a."deptno", a."commission", a.r as ar, b.r as br
                from (
                  select "empid", "deptno", "commission", first_value("empid") over w as r
                  from "emps"
                  window w as (partition by "deptno" order by "commission")) a
                join (
                  select "empid", "deptno", "commission", last_value("empid") over w as r
                  from "emps"
                  window w as (partition by "deptno" order by "commission")) b
                on a."empid" = b."empid"
                limit 5;
                +-------+--------+------------+-----+-----+
                | empid | deptno | commission | AR  | BR  |
                +-------+--------+------------+-----+-----+
                |   100 |     10 |       1000 | 110 | 100 |
                |   110 |     10 |        250 | 110 | 110 |
                |   150 |     10 |            | 110 | 150 |
                |   200 |     20 |        500 | 200 | 200 |
                +-------+--------+------------+-----+-----+
                (4 rows)""");
    }

    @Test
    public void test1() {
        this.qs("""
                select * from (
                  select "empid", count(*) over () c
                    from "emps"
                ) where "empid"=100;
                +-------+---+
                | empid | C |
                +-------+---+
                |   100 | 4 |
                +-------+---+
                (1 row)""");
    }
}
