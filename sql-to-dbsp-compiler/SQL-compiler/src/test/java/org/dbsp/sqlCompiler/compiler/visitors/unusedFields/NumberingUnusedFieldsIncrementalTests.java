package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.junit.Assert;
import org.junit.Test;

import static org.dbsp.sqlCompiler.compiler.visitors.unusedFields.NumberingUnusedFieldsTests.TABLE;
import static org.dbsp.sqlCompiler.compiler.visitors.unusedFields.NumberingUnusedFieldsTests.shape;

/** Incremental behavior of RowNumber and Rank operators with trimmed fields:
 * deleting one of two rows that the trimmed input merges renumbers the later rows. */
public class NumberingUnusedFieldsIncrementalTests extends StreamingTestBase {
    @Test
    public void testRowNumberDeleteTie() {
        var ccs = this.getCCS(TABLE + """
                CREATE LOCAL VIEW W AS
                SELECT *, ROW_NUMBER() OVER (PARTITION BY g ORDER BY ts) AS rn FROM T;
                CREATE VIEW V AS SELECT g, rn FROM W;""");
        Assert.assertEquals(new NumberingUnusedFieldsTests.Shape(2, 2), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("""
                INSERT INTO T VALUES (1, 10, 5, 'x', 0), (2, 10, 5, 'y', 1), (3, 10, 7, 'z', 0), (4, 20, 1, 'w', 0);""",
                """
                 g  | rn
                ---------
                 10 | 1
                 10 | 2
                 10 | 3
                 20 | 1""");
        ccs.step("REMOVE FROM T VALUES (1, 10, 5, 'x', 0);",
                """
                 g  | rn | weight
                ------------------
                 10 | 3  | -1""");
    }

    @Test
    public void testRankDeleteTie() {
        var ccs = this.getCCS(TABLE + """
                CREATE LOCAL VIEW W AS
                SELECT *, RANK() OVER (PARTITION BY g ORDER BY ts) AS r FROM T;
                CREATE VIEW V AS SELECT g, r FROM W;""");
        Assert.assertEquals(new NumberingUnusedFieldsTests.Shape(2, 2), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("""
                INSERT INTO T VALUES (1, 10, 5, 'x', 0), (2, 10, 5, 'y', 1), (3, 10, 7, 'z', 0), (4, 20, 1, 'w', 0);""",
                """
                 g  | r
                --------
                 10 | 1
                 10 | 1
                 10 | 3
                 20 | 1""");
        ccs.step("REMOVE FROM T VALUES (1, 10, 5, 'x', 0);",
                """
                 g  | r | weight
                -----------------
                 10 | 1 | -1
                 10 | 3 | -1
                 10 | 2 | 1""");
    }
}
