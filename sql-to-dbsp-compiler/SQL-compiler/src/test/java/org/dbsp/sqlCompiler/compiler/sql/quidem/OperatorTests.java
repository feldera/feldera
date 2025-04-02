package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Tests from Calcite quidem test operator.iq */
public class OperatorTests extends SqlIoTest {
    @Test
    public void testRow() {
        this.qs("""
                select T.X[1] as x1 from (VALUES (ROW(ROW(3, 7), ROW(4, 8)))) as T(x, y);
                +----+
                | X1 |
                +----+
                |  3 |
                +----+
                (1 row)
                
                select T.X[CAST(2 AS BIGINT)] as x2 from (VALUES (ROW(ROW(3, 7), ROW(4, 8)))) as T(x, y);
                +----+
                | X2 |
                +----+
                |  7 |
                +----+
                (1 row)
                
                select T.Y[CAST(1 AS TINYINT)] as y1 from (VALUES (ROW(ROW(3, 7), ROW(4, 8)))) as T(x, y);
                +----+
                | Y1 |
                +----+
                |  4 |
                +----+
                (1 row)
                
                select T.Y[CAST(2 AS SMALLINT)] as y2 from (VALUES (ROW(ROW(3, 7), ROW(4, 8)))) as T(x, y);
                +----+
                | Y2 |
                +----+
                |  8 |
                +----+
                (1 row)""");
    }
}
