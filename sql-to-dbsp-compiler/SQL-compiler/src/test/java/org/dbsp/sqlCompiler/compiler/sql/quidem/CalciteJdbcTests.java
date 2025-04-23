package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Ignore;
import org.junit.Test;

/** Not based on quidem tests, but on JdbcTest */
public class CalciteJdbcTests extends HrBaseTests {
    @Test @Ignore("FIRST_VALUE only supported with unbounded range")
    public void testWinAggFirstValue() {
        this.qs("""
                 select deptno, empid, commission,
                 first_value(commission) over (partition by deptno order by empid) as r
                 from emps;
                  deptno | empid | commission | R
                 ---------------------------------
                  10    | 100   | 1000       | 1000
                  10    | 110   | 250        | 1000
                  10    | 150   | null       | 1000
                  20    | 200   | 500        | 500
                 (4 rows)
                 
                 select  deptno, empid, commission,
                 first_value(commission) over (partition by deptno order by empid desc) as r
                 from emps;
                  deptno | empid | commission | R
                 ---------------------------------
                  10     | 100   | 1000       | NULL
                  10     | 110   | 250        | NULL
                  10     | 150   | NULL       | NULL
                  20     | 200   | 500        | 500
                 (4 rows)
                 
                 select deptno, empid, commission,
                 first_value(commission) over
                 (partition by deptno order by empid desc range between 1000 preceding and 999 preceding) as r
                 from emps;
                  deptno | empid | commission | R
                 -----------------------------------
                  10     | 100   | 1000       | NULL
                  10     | 110   | 250        | NULL
                  10     | 150   | null       | NULL
                  20     | 200   | 500        | NULL
                 (4 rows)""");
    }
}
