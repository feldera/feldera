package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

public class Regression2Tests extends SqlIoTest {
    @Test
    public void issue5479() {
        this.getCCS("""
                CREATE TABLE tbl(mapp MAP<VARCHAR, INT>);
                
                CREATE MATERIALIZED VIEW v AS
                SELECT mapp FROM tbl MINUS
                SELECT MAP['a', 2] FROM tbl;""");
    }

    @Test
    public void issue5481() {
        this.statementsFailingInCompilation("""
                CREATE TABLE tbl(
                str VARCHAR,
                bin BINARY,
                uuidd UUID,
                arr VARCHAR ARRAY);
                
                CREATE MATERIALIZED VIEW v1 AS
                SELECT str FROM tbl
                EXCEPT ALL
                SELECT arr[1] FROM tbl;""", "Not yet implemented: EXCEPT/MINUS ALL");
    }

    @Test
    public void intersectAllTest() {
        this.statementsFailingInCompilation("""
                CREATE TABLE T(x INT);
                CREATE VIEW V AS SELECT * FROM T INTERSECT ALL SELECT * FROM T;""", "Not yet implemented: INTERSECT ALL");
    }
}
