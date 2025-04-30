package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

public class Regression1Tests extends SqlIoTest {
    @Test
    public void issue3952() {
        var ccs = this.getCCS("""
                CREATE TABLE tbl(t0 TIMESTAMP, t1 TIMESTAMP NOT NULL);
                
                CREATE LOCAL VIEW intervalv AS SELECT
                (t0 - TIMESTAMP '2025-06-21 14:23:44') YEAR AS yr0,
                (TIMESTAMP '2025-06-21 14:23:44' - t1) YEAR AS yr1,
                (t0 - TIMESTAMP '2025-06-21 14:23:44') DAYS AS d0,
                (TIMESTAMP '2025-06-21 14:23:44' - t1) DAYS AS d1
                FROM tbl;
                
                CREATE MATERIALIZED VIEW v AS SELECT
                CAST(ABS(yr0) AS BIGINT), CAST(ABS(yr1) AS BIGINT),
                CAST(ABS(d0) AS BIGINT), CAST(ABS(d1) AS BIGINT)
                FROM intervalv;""");
        ccs.step("INSERT INTO tbl VALUES(TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2019-01-01 00:00:00')",
                """
                  v0 | v1 |  v2  |  v3  | weight
                 ----------------------------
                   5 |  6 | 1998 | 2363 | 1""");
    }
}
