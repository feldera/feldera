package org.dbsp.sqlCompiler.compiler.sql.simple;

import org.dbsp.sqlCompiler.circuit.operator.DBSPDistinctOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPStreamDistinctOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.optimizer.AntiJoinDistinctRemoveRule;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link AntiJoinDistinctRemoveRule} */
public class AntiJoinDistinctTests extends SqlIoTest {
    /** Counts the distinct operators in the circuit */
    static class CountDistinct extends CircuitVisitor {
        int count = 0;

        public CountDistinct(DBSPCompiler compiler) {
            super(compiler);
        }

        @Override
        public void postorder(DBSPDistinctOperator operator) {
            this.count++;
        }

        @Override
        public void postorder(DBSPStreamDistinctOperator operator) {
            this.count++;
        }
    }

    int countDistinct(String sql) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        var ccs = this.getCCS(compiler);
        CountDistinct counter = new CountDistinct(compiler);
        ccs.visit(counter);
        return counter.count;
    }

    static final String TABLES = """
            CREATE TABLE t(x INT NOT NULL);
            CREATE TABLE s(x INT NOT NULL);
            """;

    /** The rule removes the GROUP BY: the join result is only null-tested */
    @Test
    public void testRemoved() {
        String sql = TABLES + """
                CREATE VIEW v AS
                SELECT t.x FROM t
                LEFT JOIN (SELECT x FROM s GROUP BY x) g ON t.x = g.x
                WHERE g.x IS NULL;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        var ccs = this.getCCS(compiler);

        CountDistinct counter = new CountDistinct(compiler);
        ccs.visit(counter);
        Assert.assertEquals(0, counter.count);

        // The duplicates in s must not duplicate or revive t's rows
        ccs.stepWeightOne("""
                        INSERT INTO t VALUES(1), (2);
                        INSERT INTO s VALUES(2), (2);""",
                """
                         x
                        ---
                         1""");
    }

    /** The rule must not fire when the tested column is nullable:
     * NULL no longer proves the absence of a match. */
    @Test
    public void testNullableColumn() {
        String sql = """
                CREATE TABLE t(x INT NOT NULL);
                CREATE TABLE s(x INT);
                CREATE VIEW v AS
                SELECT t.x FROM t
                LEFT JOIN (SELECT x FROM s GROUP BY x) g ON t.x = g.x
                WHERE g.x IS NULL;""";
        Assert.assertEquals(1, this.countDistinct(sql));
    }

    /** The rule must not fire for the IS NOT NULL idiom: there the
     * GROUP BY bounds the multiplicity of the join result. */
    @Test
    public void testNotNullTest() {
        String sql = TABLES + """
                CREATE VIEW v AS
                SELECT t.x FROM t
                LEFT JOIN (SELECT x FROM s GROUP BY x) g ON t.x = g.x
                WHERE g.x IS NOT NULL;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        var ccs = this.getCCS(compiler);

        CountDistinct counter = new CountDistinct(compiler);
        ccs.visit(counter);
        Assert.assertEquals(1, counter.count);

        // Without the GROUP BY the row 2 would appear twice
        ccs.stepWeightOne("""
                        INSERT INTO t VALUES(1), (2);
                        INSERT INTO s VALUES(2), (2);""", """
                         x
                        ---
                         2""");
    }

    /** Only the GROUP BY under the IS NULL branch is removable.
     * The view selects the people who started something and never finished. */
    @Test
    public void testSegment() {
        String sql = """
                CREATE TABLE people(id VARCHAR NOT NULL PRIMARY KEY);
                CREATE TABLE events(id VARCHAR NOT NULL, kind VARCHAR);
                CREATE VIEW segment AS
                SELECT p.id FROM people p
                LEFT JOIN (SELECT id FROM events WHERE kind = 'start' GROUP BY id) s
                  ON p.id = s.id
                LEFT JOIN (SELECT id FROM events WHERE kind = 'finish' GROUP BY id) f
                  ON p.id = f.id
                WHERE s.id IS NOT NULL AND f.id IS NULL;""";
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(sql);
        var ccs = this.getCCS(compiler).withStringTrim();

        CountDistinct counter = new CountDistinct(compiler);
        ccs.visit(counter);
        Assert.assertEquals(1, counter.count);

        // 'a' finished, 'c' never started; the duplicated start events
        // of 'b' must produce a single output row, and the duplicated finish
        // events of 'a' exercise the branch whose GROUP BY the rule removed.
        // PostgreSQL 14.13 returns the same result for this data, both for
        // this query and for one without the GROUP BY in the 'finish' branch.
        ccs.stepWeightOne("""
                        INSERT INTO people VALUES('a'), ('b'), ('c');
                        INSERT INTO events VALUES
                        ('a', 'start'), ('a', 'finish'), ('a', 'finish'),
                        ('b', 'start'), ('b', 'start');""", """
                         id
                        ----
                         b""");
    }
}
