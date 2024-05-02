package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Test;

// Tests taken from
// https://github.com/apache/calcite/blob/main/babel/src/test/resources/sql/redshift.iq
public class RedshiftTests extends ScottBaseTests {
    @Test
    public void testLag() {
        this.qs("""
                select empno, lag(sal) respect nulls over (order by empno) from emp where deptno = 30 order by 1;
                EMPNO | EXPR$1
                -------------
                7499 | null
                7521 | 1600.00
                7654 | 1250.00
                7698 | 1250.00
                7844 | 2850.00
                7900 | 1500.00
                (7 rows)
                
                select empno, lag(sal, 2) respect nulls over (order by empno) from emp where deptno = 30 order by 1;
                EMPNO | EXPR$1
                -------------
                7499 | null
                7521 | null
                7654 | 1600.00
                7698 | 1250.00
                7844 | 1250.00
                7900 | 2850.00
                (6 rows)
                """, false);
    }
}
