package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Test;

/** Tests from sort.iq that use the HR database */
public class SortHrTests extends HrBaseTests {
    @Test
    public void testSort() {
        this.qs("""
                select * from "emps" offset 0;
                +-------+--------+-----------+---------+------------+
                | empid | deptno | name      | salary  | commission |
                +-------+--------+-----------+---------+------------+
                |   100 |     10 | Bill|       10000.0 |       1000 |
                |   110 |     10 | Theodore|   11500.0 |        250 |
                |   150 |     10 | Sebastian|   7000.0 |            |
                |   200 |     20 | Eric|        8000.0 |        500 |
                +-------+--------+-----------+---------+------------+
                (4 rows)
                
                select distinct "deptno", count(*) as c
                from "emps"
                group by "deptno"
                order by count(*) desc;
                +--------+---+
                | deptno | C |
                +--------+---+
                |     10 | 3 |
                |     20 | 1 |
                +--------+---+
                (2 rows)
                
                select distinct count("empid") as c
                from "emps"
                group by "empid"
                order by 1;
                +---+
                | C |
                +---+
                | 1 |
                +---+
                (1 row)

                with e as (select "empid" as empid from "emps" where "empid" < 120)
                select * from e as e1, e as e2 order by e1.empid + e2.empid, e1.empid;
                +-------+--------+
                | EMPID | EMPID0 |
                +-------+--------+
                |   100 |    100 |
                |   100 |    110 |
                |   110 |    100 |
                |   110 |    110 |
                +-------+--------+
                (4 rows)
                
                with e as (select "empid" as empid from "emps" where "empid" < 200)
                select * from e where empid > 100 limit 5;
                +-------+
                | EMPID |
                +-------+
                |   150 |
                |   110 |
                +-------+
                (2 rows)
                
                select * from
                (values
                (2, array[2, 3]),
                (3, array[3, 4]),
                (1, array[1, 2]),
                (4, array[4, 5])) as t(id, arr)
                order by arr asc;
                +----+--------+
                | ID | ARR    |
                +----+--------+
                |  1 | {1, 2} |
                |  2 | {2, 3} |
                |  3 | {3, 4} |
                |  4 | {4, 5} |
                +----+--------+
                (4 rows)
                
                select * from
                (values
                (2, array[2, 3]),
                (3, array[3, 4]),
                (1, array[1, 2]),
                (4, array[4, 5])) as t(id, arr)
                order by arr desc;
                +----+--------+
                | ID | ARR    |
                +----+--------+
                |  4 | {4, 5} |
                |  3 | {3, 4} |
                |  2 | {2, 3} |
                |  1 | {1, 2} |
                +----+--------+
                (4 rows)
                """);
    }
}
