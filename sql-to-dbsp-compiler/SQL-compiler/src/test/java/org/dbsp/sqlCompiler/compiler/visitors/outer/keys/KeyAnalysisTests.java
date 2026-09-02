package org.dbsp.sqlCompiler.compiler.visitors.outer.keys;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;

import org.dbsp.util.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.Locale;

/** Tests for {@link KeyAnalysis}, using the logger to capture changes. */
public class KeyAnalysisTests extends SqlIoTest {
    static final String TABLES = """
            CREATE TABLE t(a INT NOT NULL, b INT NOT NULL, c INT, PRIMARY KEY (a, b));
            CREATE TABLE s(x INT NOT NULL, y INT, PRIMARY KEY (x));
            CREATE TABLE m(a INT NOT NULL, b INT, c INT);
            """;

    /** Compile the program and return the analysis log */
    String compileLog(String view) {
        StringBuilder builder = new StringBuilder();
        Appendable save = Logger.INSTANCE.setDebugStream(builder);
        Logger.INSTANCE.setLoggingLevel(KeyAnalysis.class, 1);
        try {
            DBSPCompiler compiler = this.testCompiler();
            compiler.submitStatementsForCompilation(TABLES + view);
            this.getCCS(compiler);
        } finally {
            Logger.INSTANCE.setLoggingLevel(KeyAnalysis.class, 0);
            Logger.INSTANCE.setDebugStream(save);
        }
        return builder.toString();
    }

    /** Compile the program and return the keys the analysis reported for view {@code v}. */
    String viewKeys(String view) {
        String log = this.compileLog(view);
        String marker = "view v keys ";
        for (String line : log.split("\n")) {
            String lower = line.toLowerCase(Locale.ENGLISH);
            if (lower.startsWith(marker))
                return line.substring(marker.length()).trim();
        }
        throw new AssertionError("No keys reported for view v:\n" + log);
    }

    void assertKeys(String view, String expected) {
        Assert.assertEquals(expected, this.viewKeys(view));
    }

    void assertNoKeys(String view) {
        this.assertKeys(view, "[]");
    }

    /////////////////////////////////////////////////////////////
    // What the analysis derives for each SQL construct

    @Test
    public void primaryKey() {
        this.assertKeys("CREATE VIEW v AS SELECT * FROM t;", "[[0, 1]]");
    }

    @Test
    public void tableWithoutKey() {
        this.assertNoKeys("CREATE VIEW v AS SELECT * FROM m;");
    }

    @Test
    public void projectionRenamesKey() {
        this.assertKeys("CREATE VIEW v AS SELECT c, b, a FROM t;", "[[1, 2]]");
    }

    @Test
    public void projectionDroppingKeyColumn() {
        this.assertNoKeys("CREATE VIEW v AS SELECT a, c FROM t;");
    }

    /** a + 1 is injective, but the analysis only follows plain copies. */
    @Test
    public void computedColumnBreaksKey() {
        this.assertNoKeys("CREATE VIEW v AS SELECT a + 1 AS a1, b FROM t;");
    }

    @Test
    public void filterKeepsKey() {
        this.assertKeys("CREATE VIEW v AS SELECT * FROM t WHERE c > 0;", "[[0, 1]]");
    }

    /** b is the same on every remaining row, so a alone identifies the row. */
    @Test
    public void filterFixingKeyColumn() {
        this.assertKeys("CREATE VIEW v AS SELECT a, b, c FROM t WHERE b = 3;", "[[0]]");
    }

    /** Same through a nullable comparison, on a key that comes from a TOP-1. */
    @Test
    public void filterFixingNullableKeyColumn() {
        this.assertKeys("""
                CREATE LOCAL VIEW w AS SELECT a, b, c FROM m
                QUALIFY row_number() OVER (PARTITION BY a, b ORDER BY c) = 1;
                CREATE VIEW v AS SELECT a, b, c FROM w WHERE b = 2;""", "[[0]]");
    }

    /** Only top-level conjuncts fix a column. */
    @Test
    public void filterWithOrFixesNothing() {
        this.assertKeys("CREATE VIEW v AS SELECT a, b, c FROM t WHERE b = 3 OR c = 1;", "[[0, 1]]");
    }

    @Test
    public void distinctIsKey() {
        this.assertKeys("CREATE VIEW v AS SELECT DISTINCT b, c FROM m;", "[[0, 1]]");
    }

    /** A column selected twice holds the same data in both places, so the key a distinct adds
     * names that data once instead of listing both columns. */
    @Test
    public void distinctKeyCollapsesEqualColumns() {
        this.assertKeys("CREATE VIEW v AS SELECT DISTINCT a, a AS a2, b FROM m;", "[[0=1, 2]]");
    }

    /** The rows of a distinct are a subset of its input's, so a key of the input still
     * identifies a row and outlives the longer key over all the columns. */
    @Test
    public void distinctKeepsTheSmallerInputKey() {
        this.assertKeys("CREATE VIEW v AS SELECT DISTINCT a, b, c FROM t;", "[[0, 1]]");
    }

    @Test
    public void groupByIsKey() {
        this.assertKeys("CREATE VIEW v AS SELECT a, COUNT(*) AS cnt FROM m GROUP BY a;", "[[0]]");
    }

    /** A global aggregate produces a single row, with an empty key. */
    @Test
    public void globalAggregateHasOneRow() {
        this.assertKeys("CREATE VIEW v AS SELECT COUNT(*) AS n, SUM(c) AS total FROM m;", "[[]]");
    }

    /** The join equates t.a with s.x, and both are group columns, so the key of the
     * aggregate names that value once instead of listing both columns. */
    @Test
    public void groupByEqualColumnsCollapses() {
        this.assertKeys("""
                CREATE VIEW v AS SELECT t.a, s.x, COUNT(*) AS n
                FROM t JOIN s ON t.a = s.x GROUP BY t.a, s.x;""", "[[0=1]]");
    }

    @Test
    public void top1RowNumberIsKey() {
        this.assertKeys("""
                CREATE VIEW v AS SELECT a, b, c FROM m
                QUALIFY row_number() OVER (PARTITION BY a ORDER BY b) = 1;""", "[[0]]");
    }

    /** RANK admits ties: several rows per partition -- no key. */
    @Test
    public void top1RankIsNotKey() {
        this.assertNoKeys("""
                CREATE VIEW v AS SELECT a, b, c FROM m
                QUALIFY rank() OVER (PARTITION BY a ORDER BY b) = 1;""");
    }

    @Test
    public void top2IsNotKey() {
        this.assertNoKeys("""
                CREATE VIEW v AS SELECT a, b, c FROM m
                QUALIFY row_number() OVER (PARTITION BY a ORDER BY b) <= 2;""");
    }

    /** A column selected twice is a key on each of its copies. */
    @Test
    public void top1WithDuplicatedPartitionColumn() {
        // 0=1 is an equivalence set: both columns carry the same value
        this.assertKeys("""
                CREATE VIEW v AS SELECT a, a AS a2, b FROM m
                QUALIFY row_number() OVER (PARTITION BY a ORDER BY b) = 1;""", "[[0=1]]");
    }

    /** Both occurrences of the expression "a+b" compute the same
     * value, which is propagated as a key by the partition key. */
    @Test
    public void top1OnComputedColumn() {
        this.assertKeys("""
                CREATE VIEW v AS SELECT a + b AS ab, c FROM m
                QUALIFY row_number() OVER (PARTITION BY a + b ORDER BY c) = 1;""", "[[0]]");
    }

    /** Two different computations of the same columns are not known to hold the same value. */
    @Test
    public void top1OnOtherComputedColumn() {
        this.assertNoKeys("""
                CREATE VIEW v AS SELECT a - b AS ab, c FROM m
                QUALIFY row_number() OVER (PARTITION BY a + b ORDER BY c) = 1;""");
    }

    /** The TOP-1 output is indexed by the seven partition columns and repeats them in its
     * value tuple.  Each value of the key is named by both its columns. */
    @Test
    public void wideKeyOfDuplicatedColumnsStaysOneKey() {
        String view = """
                CREATE TABLE w(c1 INT NOT NULL, c2 INT NOT NULL, c3 INT NOT NULL, c4 INT NOT NULL,
                               c5 INT NOT NULL, c6 INT NOT NULL, c7 INT NOT NULL, x INT);
                CREATE VIEW v AS SELECT c1, c2, c3, c4, c5, c6, c7, x FROM w
                QUALIFY row_number() OVER (PARTITION BY c1, c2, c3, c4, c5, c6, c7 ORDER BY x) = 1;""";
        // The TOP-1 partition columns are its index, and its value tuple repeats them, so
        // each of the seven values of the key is named by an index and a value column
        // This is an approximation of the true key, which has 2^7 members
        String log = this.compileLog(view);
        Assert.assertTrue(log, log.contains("[i0=v0, i1=v1, i2=v2, i3=v3, i4=v4, i5=v5, i6=v6]"));
        this.assertKeys(view, "[[0, 1, 2, 3, 4, 5, 6]]");
    }

    /** Two independent keys: the primary key of the table, and the partition column of a
     * TOP-1, which keeps one row per value of it.  Neither contains the other. */
    @Test
    public void twoIndependentKeys() {
        this.assertKeys("""
                CREATE VIEW v AS SELECT a, b, c FROM t
                QUALIFY row_number() OVER (PARTITION BY c ORDER BY a) = 1;""", "[[2], [0, 1]]");
    }

    /** The join key covers the key of s: every t row matches at most once. */
    @Test
    public void joinOnRightKey() {
        this.assertKeys("CREATE VIEW v AS SELECT t.a, t.b, t.c, s.y FROM t JOIN s ON t.c = s.x;", "[[0, 1]]");
    }

    @Test
    public void leftJoinOnRightKey() {
        this.assertKeys("CREATE VIEW v AS SELECT t.a, t.b, t.c, s.y FROM t LEFT JOIN s ON t.c = s.x;", "[[0, 1]]");
    }

    /** Neither side is keyed by the join column: only the pair of keys identifies a row. */
    @Test
    public void joinOnNonKey() {
        this.assertKeys("CREATE VIEW v AS SELECT t.a, t.b, s.x FROM t JOIN s ON t.c = s.y;", "[[0, 1, 2]]");
    }

    /** The pair rule needs the right key, which is not in the output. */
    @Test
    public void joinDroppingRightKey() {
        this.assertNoKeys("CREATE VIEW v AS SELECT t.a, t.b FROM t JOIN s ON t.c = s.y;");
    }

    /** Two joins on the same key: each lookup matches at most one row, so the key of the
     * probing input identifies an output row on its own. */
    @Test
    public void joinsOnOneSharedKey() {
        this.assertKeys("""
                CREATE VIEW v AS SELECT t.a, t.b, s.y, u.y AS y2
                FROM t JOIN s ON t.a = s.x JOIN s AS u ON t.a = u.x;""", "[[0, 1]]");
    }

    /** The join equates t.a with s.x, and the view names the value a third time, so one set
     * of the key holds all three columns. */
    @Test
    public void joinEqualityWidensASet() {
        this.assertKeys("""
                CREATE VIEW v AS SELECT t.a, s.x, s.x AS x2, t.b
                FROM t JOIN s ON t.a = s.x;""", "[[0=1=2, 3]]");
    }

    /** An operator can carry an expression that is not a closure, a comparator here, which
     * the analysis must step over rather than read as a function over rows. */
    @Test
    public void orderByLimitIsAnalyzed() {
        this.assertKeys("CREATE VIEW v AS SELECT a, b, c FROM t ORDER BY c LIMIT 2;", "[[0, 1]]");
    }

    /** A LEFT JOIN without an equality is expanded into a join and an antijoin whose outputs
     * are added.  The right side here holds a single row, so each left row matches at most
     * once and both branches keep the key of t, as the two assertions on the log show.  The
     * branches hold disjoint sets of left rows, so (a, b) is a key of their sum as well, but
     * no rule makes a key out of a sum and the view reports none.  Recognizing the expansion
     * would report [[0, 1]] here. */
    @Test
    public void nonEquiLeftJoinLosesTheKeyAtTheUnion() {
        String view = """
                CREATE VIEW v AS SELECT t.a, t.b, g.mx
                FROM t LEFT JOIN (SELECT MAX(x) AS mx FROM s) g ON t.c < g.mx;""";
        String log = this.compileLog(view);
        Assert.assertTrue(log, log.contains("stream_join keys [[0, 1]]"));
        Assert.assertTrue(log, log.contains("stream_antijoin keys [[v0, v1]]"));
        this.assertNoKeys(view);
    }

    @Test
    public void unionHasNoKey() {
        this.assertNoKeys("CREATE VIEW v AS SELECT a, b FROM t UNION ALL SELECT x, y FROM s;");
    }
}
