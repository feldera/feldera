package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Test;

/** Quidem tests from postgres.iq */
public class PostgresTests extends SqlIoTest {
    @Test
    public void testGreatestIgnoreNulls() {
        this.qs("""
                SELECT greatest_ignore_nulls(1, 2, 3) as x;
                 X
                ---
                 3
                (1 row)
                
                SELECT greatest_ignore_nulls(1, null, 3) AS x;
                 X
                ---
                 3
                (1 row)
                
                SELECT least_ignore_nulls(1, 2, 3) AS x;
                 X
                ---
                 1
                (1 row)
                
                SELECT least_ignore_nulls(1, null, 3) AS x;
                 X
                ---
                 1
                (1 row)
                
                SELECT least_ignore_nulls(null, cast(null as DATE));
                 X
                ----
                NULL
                (1 row)""");
    }

    @Test
    public void testGreatest() {
        this.qs("""
                SELECT greatest(1, 2, 3) as x;
                 X
                ---
                 3
                (1 row)
                
                SELECT greatest(1, null, 3) AS x;
                 X
                ---
                NULL
                (1 row)
                
                SELECT least(1, 2, 3) AS x;
                 X
                ---
                 1
                (1 row)
                
                SELECT least(1, null, 3) AS x;
                 X
                ---
                NULL
                (1 row)
                
                SELECT greatest(null, cast(null as DATE));
                 X
                ---
                NULL
                (1 row)""");
    }

    @Test
    public void testGreatest2() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT, y INT NOT NULL, z DECIMAL(10, 2));
                CREATE VIEW V AS SELECT GREATEST(x, y, z), LEAST(x, y, z) FROM T;""");
        ccs.step("INSERT INTO T VALUES(0, 1, 2), (NULL, 1, 2), (2, 3, NULL)", """
                 greatest | least | weight
                ---------------------------
                 2        | 0     | 1
                          |       | 1
                          |       | 1""");
    }

    @Test
    public void testGreatest3() {
        var ccs = this.getCCS("""
                CREATE TABLE T(x INT, y INT NOT NULL, z DECIMAL(10, 2));
                CREATE VIEW V AS SELECT GREATEST_IGNORE_NULLS(x, y, z), LEAST_IGNORE_NULLS(x, y, z) FROM T;""");
        ccs.step("INSERT INTO T VALUES(0, 1, 2), (NULL, 1, 2), (2, 3, NULL)", """
                 greatest | least | weight
                ---------------------------
                 2        | 0     | 1
                 2        | 1     | 1
                 3        | 2     | 1""");
    }
}
