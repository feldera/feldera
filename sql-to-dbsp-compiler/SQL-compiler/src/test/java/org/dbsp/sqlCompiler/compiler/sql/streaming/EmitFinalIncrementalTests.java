package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.junit.Test;

/** Views with an 'emit_final' annotation. */
public class EmitFinalIncrementalTests extends StreamingTestBase {
    /** A row whose emit_final column is NULL is never late, so it is never final and never
     * emitted, even after the waterline passes every other row. */
    @Test
    public void nullIsNeverEmitted() {
        var ccs = this.getCCS("""
                CREATE TABLE T (ts BIGINT LATENESS 0, x INT);
                CREATE VIEW V WITH ('emit_final' = 'ts') AS SELECT ts, x FROM T;""");
        ccs.step("INSERT INTO T VALUES (NULL, 1), (1, 2);", """
                 ts | x | weight
                -----------------""");
        ccs.step("INSERT INTO T VALUES (5, 3);", """
                 ts | x | weight
                -----------------
                 1  | 2 | 1""");
        ccs.step("INSERT INTO T VALUES (10, 4);", """
                 ts | x | weight
                -----------------
                 5  | 3 | 1""");
        // The NULL row was never emitted, so deleting it emits nothing
        ccs.step("REMOVE FROM T VALUES (NULL, 1);", """
                 ts | x | weight
                -----------------""");
    }

    @Test
    public void issue7372() {
        this.statementsFailingInCompilation("""
                CREATE TABLE S (COL1 INT LATENESS 1);
                CREATE VIEW W WITH ('emit_final' = 'col1') AS SELECT COL1, COUNT(*) AS C FROM S GROUP BY COL1;
                CREATE INDEX IX ON W(COL1);""", """
                3:20: error: Not supported: View 'w' has an 'emit_final' property, so it cannot have INDEX 'ix'.
                    2|CREATE VIEW W WITH ('emit_final' = 'col1') AS SELECT COL1, COUNT(*) AS C FROM S GROUP BY COL1;
                    3|CREATE INDEX IX ON W(COL1);
                                         ^""");
    }
}
