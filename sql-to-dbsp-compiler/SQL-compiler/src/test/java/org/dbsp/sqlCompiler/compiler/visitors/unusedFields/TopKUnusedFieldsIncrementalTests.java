package org.dbsp.sqlCompiler.compiler.visitors.unusedFields;

import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.junit.Assert;
import org.junit.Test;

import static org.dbsp.sqlCompiler.compiler.visitors.unusedFields.TopKUnusedFieldsTests.TABLE;
import static org.dbsp.sqlCompiler.compiler.visitors.unusedFields.TopKUnusedFieldsTests.shape;

/** Incremental behavior of TopK operators with trimmed fields:
 * later changes must retract replaced rows and promote runners-up. */
public class TopKUnusedFieldsIncrementalTests extends StreamingTestBase {
    /** A newer version replaces the top row; deleting it restores the previous one. */
    @Test
    public void testPromotion() {
        var ccs = this.getCCS(TABLE + """
                CREATE VIEW V AS
                SELECT id, a FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS rn FROM T)
                WHERE rn = 1;""");
        Assert.assertEquals(new TopKUnusedFieldsTests.Shape(3, 2, false), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("INSERT INTO T VALUES (1, 10, 100, 0, 0), (1, 20, 200, 0, 0), (2, 5, 500, 0, 0);",
                """
                 id | a
                --------
                 1  | 200
                 2  | 500""");
        ccs.step("INSERT INTO T VALUES (1, 30, 300, 0, 0);",
                """
                 id | a   | weight
                -------------------
                 1  | 200 | -1
                 1  | 300 | 1""");
        ccs.step("REMOVE FROM T VALUES (1, 30, 300, 0, 0);",
                """
                 id | a   | weight
                -------------------
                 1  | 300 | -1
                 1  | 200 | 1""");
    }

    /** A deletion record that arrives later removes the version it supersedes. */
    @Test
    public void testLateSoftDelete() {
        var ccs = this.getCCS("""
                CREATE TABLE changes(
                    k INT NOT NULL, seq INT NOT NULL, deleted BOOLEAN,
                    name VARCHAR, qty INT, kind VARCHAR, note VARCHAR);
                CREATE LOCAL VIEW latest AS
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY seq DESC, deleted NULLS FIRST) AS rn
                    FROM changes)
                WHERE rn = 1 AND deleted IS NOT TRUE;
                CREATE VIEW special AS
                SELECT name, qty FROM latest WHERE kind LIKE '%special%';""").withStringTrim();
        Assert.assertEquals(new TopKUnusedFieldsTests.Shape(5, 4, false), shape(ccs));
        // Expected output validated with Postgres 14.
        ccs.stepWeightOne("""
                INSERT INTO changes VALUES
                 (7, 1, FALSE, 'n1', 10, 'special-a', 'x'),
                 (7, 2, FALSE, 'n1', 20, 'special-a', 'x'),
                 (8, 1, FALSE, 'n2', 30, 'plain', 'x');""",
                """
                 name | qty
                ------------
                 n1   | 20""");
        ccs.step("INSERT INTO changes VALUES (7, 3, TRUE, NULL, NULL, NULL, NULL);",
                """
                 name | qty | weight
                ---------------------
                 n1   | 20  | -1""");
    }
}
