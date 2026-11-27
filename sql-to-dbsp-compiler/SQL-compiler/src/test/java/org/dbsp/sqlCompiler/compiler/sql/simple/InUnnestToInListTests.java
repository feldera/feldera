package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer.InUnnestToInListRule;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link InUnnestToInListRule} */
public class InUnnestToInListTests extends SqlIoTest {
    /** Counts the join operators in the circuit */
    static class CountJoins extends CircuitVisitor {
        int count = 0;

        CountJoins(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPJoinBaseOperator operator) {
            this.count++;
        }
    }

    static int countJoins(CompilerCircuitStream ccs) {
        CountJoins counter = new CountJoins(ccs.compiler);
        ccs.visit(counter);
        return counter.count;
    }

    static final String TABLES = """
            CREATE TABLE t(id INT NOT NULL, x INT);
            CREATE TABLE s(id INT NOT NULL);
            """;

    static final String INSERT = """
            INSERT INTO t VALUES(1, 1), (2, 4), (3, NULL);
            INSERT INTO s VALUES(1), (3);""";

    /** IN and NOT IN over a constant array become IN lists, which compile to filters.
     * The results keep the three-valued logic: NULL IN (...) is NULL.  Validated on Postgres. */
    @Test
    public void testRewritten() {
        var ccs = this.getCCS(TABLES + """
                CREATE VIEW v AS SELECT id, x,
                    x IN (SELECT * FROM UNNEST(ARRAY[1, 2, 3])) AS i,
                    x NOT IN (SELECT * FROM UNNEST(ARRAY[1, 2, 3])) AS n
                FROM t;""");
        Assert.assertEquals(0, countJoins(ccs));
        ccs.stepWeightOne(INSERT, """
                 id | x | i     | n
                -----------------------
                 1  | 1 | true  | false
                 2  | 4 | false | true
                 3  |NULL|NULL|NULL""");
    }

    /** The rule rewrites the IN over the array and leaves the other sub-query of the same filter */
    @Test
    public void testOtherSubQuery() {
        var ccs = this.getCCS(TABLES + """
                CREATE VIEW v AS SELECT id, x FROM t
                WHERE id IN (SELECT id FROM s) AND x IN (SELECT * FROM UNNEST(ARRAY[1, 4]));""");
        Assert.assertEquals(1, countJoins(ccs));
        ccs.stepWeightOne(INSERT, """
                 id | x
                --------
                 1  | 1""");
    }

    /** An array with one element becomes an equality.  Validated on Postgres. */
    @Test
    public void testSingleElement() {
        var ccs = this.getCCS(TABLES + """
                CREATE VIEW v AS SELECT id FROM t WHERE x IN (SELECT * FROM UNNEST(ARRAY[4]));""");
        Assert.assertEquals(0, countJoins(ccs));
        ccs.stepWeightOne(INSERT, """
                 id
                ----
                 2""");
    }

    /** For a BIGINT column the validator casts the INTEGER elements of the sub-query to BIGINT;
     * the IN list keeps the cast.  Validated on Postgres. */
    @Test
    public void testWiderInteger() {
        var ccs = this.getCCS("""
                CREATE TABLE b(id INT NOT NULL, y BIGINT);
                CREATE VIEW v AS SELECT id FROM b WHERE y IN (SELECT * FROM UNNEST(ARRAY[1, 4]));""");
        Assert.assertEquals(0, countJoins(ccs));
        ccs.stepWeightOne("INSERT INTO b VALUES(1, 1), (2, 4), (3, 5000000000), (4, NULL);", """
                 id
                ----
                 1
                 2""");
    }

    /** For a DECIMAL column the validator casts x and the sub-query column to a common DECIMAL
     * type, and the sub-query column twice.  Validated on Postgres. */
    @Test
    public void testDecimal() {
        var ccs = this.getCCS("""
                CREATE TABLE d(id INT NOT NULL, x DECIMAL(5, 2));
                CREATE VIEW v AS SELECT id FROM d WHERE x IN (SELECT * FROM UNNEST(ARRAY[1, 4]));""");
        Assert.assertEquals(0, countJoins(ccs));
        ccs.stepWeightOne("INSERT INTO d VALUES(1, 1.00), (2, 4.50), (3, NULL), (4, 4);", """
                 id
                ----
                 1
                 4""");
    }

    /** CHAR literals compared with a VARCHAR column are cast to VARCHAR; the shorter literal
     * is not padded, so 'a ' does not match 'a'.  Validated on Postgres. */
    @Test
    public void testVarchar() {
        var ccs = this.getCCS("""
                CREATE TABLE c(id INT NOT NULL, z VARCHAR);
                CREATE VIEW v AS SELECT id FROM c WHERE z IN (SELECT * FROM UNNEST(ARRAY['a', 'bc']));""");
        Assert.assertEquals(0, countJoins(ccs));
        ccs.stepWeightOne("INSERT INTO c VALUES(1, 'a'), (2, 'a '), (3, 'bc'), (4, 'b');", """
                 id
                ----
                 1
                 3""");
    }

    /** The rule does not handle an array with a NULL element.
     * 4 NOT IN (1, NULL) is NULL, so no row qualifies.  Validated on Postgres. */
    @Test
    public void testNullElement() {
        var ccs = this.getCCS(TABLES + """
                CREATE VIEW v AS SELECT id, x FROM t
                WHERE x NOT IN (SELECT * FROM UNNEST(ARRAY[1, NULL]));""");
        Assert.assertNotEquals(0, countJoins(ccs));
        ccs.stepWeightOne(INSERT, """
                 id | x
                --------""");
    }
}
