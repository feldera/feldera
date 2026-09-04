package org.dbsp.sqlCompiler.compiler.visitors.outer;

import org.dbsp.sqlCompiler.circuit.operator.DBSPJoinBaseOperator;
import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link RemoveUselessLeftJoins} */
public class RemoveUselessLeftJoinsTests extends SqlIoTest {
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

    static final String TABLES = """
            CREATE TABLE t(a INT NOT NULL, b INT NOT NULL, c INT, PRIMARY KEY (a, b));
            CREATE TABLE s(x INT NOT NULL, y INT, PRIMARY KEY (x));
            CREATE TABLE m(a INT NOT NULL, b INT);
            CREATE TABLE f(x INT NOT NULL, k INT NOT NULL, z INT, PRIMARY KEY (x, k));
            """;

    CompilerCircuitStream compile(String view) {
        DBSPCompiler compiler = this.testCompiler();
        compiler.submitStatementsForCompilation(TABLES + view);
        return this.getCCS(compiler);
    }

    /** True if this pass removed a join, rather than Calcite having done it earlier: only
     * this pass reports that warning. */
    boolean passRemovedAJoin(CompilerCircuitStream ccs) {
        for (var message : ccs.compiler.messages.messages)
            if (message.warning && message.errorType.equals("LEFT JOIN has no effect"))
                return true;
        return false;
    }

    int joins(CompilerCircuitStream ccs) {
        CountJoins counter = new CountJoins(ccs.compiler);
        ccs.visit(counter);
        return counter.count;
    }

    /** s is keyed by x and none of its columns is read: the join goes, and every t row
     * survives once, including the one without a match.  Results validated using postgres. */
    @Test
    public void removed() {
        CompilerCircuitStream ccs = this.compile(
                "CREATE VIEW v AS SELECT t.a, t.b, t.c FROM t LEFT JOIN s ON t.c = s.x;");
        Assert.assertEquals(0, this.joins(ccs));
        Assert.assertTrue(this.passRemovedAJoin(ccs));
        ccs.stepWeightOne("""
                        INSERT INTO t VALUES (1, 1, 5), (2, 2, NULL), (3, 3, 9);
                        INSERT INTO s VALUES (5, 10);""",
                """
                         a | b | c
                        -----------
                         1 | 1 | 5
                         2 | 2 |NULL
                         3 | 3 | 9""");
    }

    /** The right key comes from a TOP-1 pushed into the join's right input.  Partition 5 holds
     * three rows, so a rewrite that lost the rn = 1 filter would emit the first t row 3 times.
     * Results validated using postgres. */
    @Test
    public void removedWithTop1Key() {
        CompilerCircuitStream ccs = this.compile("""
                CREATE VIEW v AS SELECT t.a, t.b, t.c FROM t
                LEFT JOIN (SELECT a, b, row_number() OVER (PARTITION BY a ORDER BY b) AS rn FROM m) d
                       ON d.a = t.c AND d.rn = 1;""");
        Assert.assertEquals(0, this.joins(ccs));
        Assert.assertTrue(this.passRemovedAJoin(ccs));
        ccs.stepWeightOne("""
                        INSERT INTO t VALUES (1, 1, 5), (2, 2, NULL), (3, 3, 9);
                        INSERT INTO m VALUES (5, 1), (5, 2), (5, 3);""",
                """
                         a | b | c
                        -----------
                         1 | 1 | 5
                         2 | 2 |NULL
                         3 | 3 | 9""");
    }

    /** A filter fixing one column of a two-column key leaves the other as a key.
     * Results validated using postgres. */
    @Test
    public void removedWhenFilterFixesAKeyColumn() {
        CompilerCircuitStream ccs = this.compile("""
                CREATE VIEW v AS SELECT t.a, t.b, t.c FROM t
                LEFT JOIN (SELECT x, z FROM f WHERE k = 3) g ON g.x = t.c;""");
        Assert.assertEquals(0, this.joins(ccs));
        Assert.assertTrue(this.passRemovedAJoin(ccs));
        ccs.stepWeightOne("""
                        INSERT INTO t VALUES (1, 1, 5), (2, 2, NULL);
                        INSERT INTO f VALUES (5, 3, 100), (5, 4, 200);""",
                """
                         a | b | c
                        -----------
                         1 | 1 | 5
                         2 | 2 |NULL""");
    }

    /** A GROUP BY produces one row per group, which keys the right input.
     * PROJECT_JOIN_REMOVE rule removes this join.
     * Results validated using postgres. */
    @Test
    public void removedWhenRightIsGrouped() {
        CompilerCircuitStream ccs = this.compile("""
                CREATE VIEW v AS SELECT t.a, t.b, t.c FROM t
                LEFT JOIN (SELECT a, COUNT(*) AS n FROM m GROUP BY a) g ON g.a = t.c;""");
        Assert.assertEquals(0, this.joins(ccs));
        ccs.stepWeightOne("""
                        INSERT INTO t VALUES (1, 1, 5), (2, 2, NULL);
                        INSERT INTO m VALUES (5, 1), (5, 2);""",
                """
                         a | b | c
                        -----------
                         1 | 1 | 5
                         2 | 2 |NULL""");
    }

    /** The left input is a multiset: the rewrite must keep each row's weight.  Results
     * validated using postgres, which repeats a row instead of weighing it. */
    @Test
    public void removedKeepsLeftMultiplicity() {
        CompilerCircuitStream ccs = this.compile(
                "CREATE VIEW v AS SELECT m.a, m.b FROM m LEFT JOIN s ON m.a = s.x;");
        Assert.assertEquals(0, this.joins(ccs));
        Assert.assertTrue(this.passRemovedAJoin(ccs));
        ccs.step("""
                        INSERT INTO m VALUES (1, 1), (1, 1), (2, 2);
                        INSERT INTO s VALUES (1, 10);""",
                """
                         a | b | weight
                        ----------------
                         1 | 1 | 2
                         2 | 2 | 1""");
    }

    /** A LEFT JOIN with no equality is expanded into a join and an antijoin, which the pass
     * does not match, so its rows keep coming from the join. */
    @Test
    public void nonEquiJoinKept() {
        CompilerCircuitStream ccs = this.compile(
                "CREATE VIEW v AS SELECT t.a, t.b, t.c FROM t LEFT JOIN s ON t.c < s.x;");
        Assert.assertEquals(1, this.joins(ccs));
    }

    /** s.y is not a key of s: a t row may match several times. */
    @Test
    public void keptWhenRightNotKeyed() {
        CompilerCircuitStream ccs = this.compile(
                "CREATE VIEW v AS SELECT t.a, t.b, t.c FROM t LEFT JOIN s ON t.c = s.y;");
        Assert.assertEquals(1, this.joins(ccs));
    }

    @Test
    public void keptWhenRightColumnsUsed() {
        CompilerCircuitStream ccs = this.compile(
                "CREATE VIEW v AS SELECT t.a, t.b, s.y FROM t LEFT JOIN s ON t.c = s.x;");
        Assert.assertEquals(1, this.joins(ccs));
    }

    /** An inner join drops the unmatched rows, so it stays even when keyed and unread. */
    @Test
    public void innerJoinKept() {
        CompilerCircuitStream ccs = this.compile(
                "CREATE VIEW v AS SELECT t.a, t.b, t.c FROM t JOIN s ON t.c = s.x;");
        Assert.assertEquals(1, this.joins(ccs));
    }
}
