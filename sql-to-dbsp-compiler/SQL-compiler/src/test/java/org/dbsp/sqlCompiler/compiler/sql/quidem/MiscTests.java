package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.junit.Ignore;
import org.junit.Test;

/** Tests from Calcite misc.iq */
public class MiscTests extends ScottBaseTests {
    @Test
    public void rowTests() {
        this.qs("""
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

    @Test @Ignore("Requires MULTISET")
    public void testRowCoalesce() {
        this.qs("""
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
}
