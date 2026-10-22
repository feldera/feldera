package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainKeysOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainNValuesOperator;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.sql.tools.CountGCOperators;
import org.dbsp.sqlCompiler.compiler.sql.tools.DifferentialTester;
import org.dbsp.sqlCompiler.compiler.sql.tools.TablePair;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/** MergeGC shares one integral among the consumers of a stream and merges their retention policies. */
public class MergeGCIncrementalTests extends StreamingTestBase {
    /** T joins S1 and S2 on ts; each join retains T with the waterline of the other side. */
    static final String TWO_JOINS = """
            CREATE TABLE T (ts INT NOT NULL, v INT);
            CREATE TABLE S1 (ts INT NOT NULL LATENESS 0, x INT);
            CREATE TABLE S2 (ts INT NOT NULL LATENESS 100, y INT);
            CREATE LOCAL VIEW V1 AS SELECT T.ts, T.v, S1.x FROM T JOIN S1 ON T.ts = S1.ts;
            CREATE LOCAL VIEW V2 AS SELECT T.ts, T.v, S2.y FROM T JOIN S2 ON T.ts = S2.ts;
            CREATE VIEW V AS SELECT 1 AS w, ts, v, x AS o FROM V1
            UNION ALL SELECT 2 AS w, ts, v, y AS o FROM V2;""";

    /** The two retain operators of T have different bounds; they are merged into one operator
     * that keeps a key when either keeps it. */
    @Test
    public void differentBoundsMerged() {
        CompilerCircuit cc = this.getCC(TWO_JOINS);
        cc.visit(new CircuitVisitor(cc.compiler) {
            int retainKeys = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainKeysOperator operator) {
                this.retainKeys++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.retainKeys);
            }
        });
    }

    /** X joins Y1 and Y2 on (t1, t2); only t1 of Y1 and only t2 of Y2 have LATENESS.  The two
     * RetainKeys of X compare different key fields; they are merged into one that keeps a key
     * when either keeps it. */
    @Test
    public void differentKeyFieldsMerged() {
        CompilerCircuit cc = this.getCC("""
                CREATE TABLE X (t1 INT NOT NULL, t2 INT NOT NULL, v INT);
                CREATE TABLE Y1 (t1 INT NOT NULL LATENESS 0, t2 INT NOT NULL, w INT);
                CREATE TABLE Y2 (t1 INT NOT NULL, t2 INT NOT NULL LATENESS 0, w INT);
                CREATE VIEW V1 AS SELECT X.v, Y1.w FROM X JOIN Y1 ON X.t1 = Y1.t1 AND X.t2 = Y1.t2;
                CREATE VIEW V2 AS SELECT X.v, Y2.w FROM X JOIN Y2 ON X.t1 = Y2.t1 AND X.t2 = Y2.t2;""");
        CountGCOperators operators = new CountGCOperators(cc.compiler);
        cc.visit(operators);
        Assert.assertEquals("K", operators.kinds());
    }

    @Test
    public void issue7584() {
        CompilerCircuit cc = this.getCC("""
                CREATE TABLE T (ts INT NOT NULL, v INT);
                CREATE TABLE S (ts INT LATENESS 0, x INT);
                CREATE VIEW V1 AS SELECT T.ts, T.v, S.x FROM T JOIN S ON T.ts = S.ts;
                CREATE VIEW V2 AS SELECT T.ts, T.v FROM T JOIN (SELECT ts FROM S) S ON T.ts = S.ts;""");
        CountGCOperators operators = new CountGCOperators(cc.compiler);
        cc.visit(operators);
        Assert.assertEquals("K", operators.kinds());
    }

    @Test
    public void issue7584Original() {
        this.getCCS("""
                CREATE TABLE L (k INT NOT NULL, t INT LATENESS 10, v INT NOT NULL);
                CREATE TABLE R (k INT NOT NULL, t INT, v INT NOT NULL);
                CREATE VIEW V1 AS SELECT l.k, l.t, r.v FROM L l JOIN R r ON l.t = r.t;
                CREATE VIEW V2 AS SELECT l.t, r.v FROM (SELECT t FROM L) l JOIN R r ON l.t = r.t;""");
    }

    @Test
    public void issue7584ThreeTypes() {
        this.getCCS("""
                CREATE TABLE T (ts INT NOT NULL, v INT);
                CREATE TABLE S (ts INT LATENESS 0, x INT);
                CREATE TABLE Q (ts INT NOT NULL LATENESS 0, y VARCHAR, z INT);
                CREATE VIEW V1 AS SELECT T.ts, T.v, S.x FROM T JOIN S ON T.ts = S.ts;
                CREATE VIEW V2 AS SELECT T.ts, T.v FROM T JOIN (SELECT ts FROM S) S ON T.ts = S.ts;
                CREATE VIEW V3 AS SELECT T.ts, T.v, Q.y FROM T JOIN Q ON T.ts = Q.ts;
                CREATE VIEW V4 AS SELECT T.ts, T.v FROM T JOIN (SELECT ts, z FROM Q) Q ON T.ts = Q.ts;""");
    }

    /** Both views compute MAX(x) grouped by ts, so the two retain operators of x keep the same
     * values with the same bounds, and they are merged. */
    @Test
    public void samePolicyMerged() {
        CompilerCircuit cc = this.getCC("""
                CREATE TABLE T (ts INT, x INT LATENESS 2, y INT);
                CREATE VIEW V1 AS SELECT ts, MAX(x) FROM T GROUP BY ts;
                CREATE VIEW V2 AS SELECT ts, MAX(x), SUM(y) FROM T GROUP BY ts;""");
        cc.visit(new CircuitVisitor(cc.compiler) {
            int retainNValues = 0;

            @Override
            public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
                this.retainNValues++;
            }

            @Override
            public void endVisit() {
                Assert.assertEquals(1, this.retainNValues);
            }
        });
    }

    /** MIN retains the smallest values below the waterline and MAX the largest; their retain
     * operators have the same function and bounds but must not be merged.  The program runs with
     * and without LATENESS; no input row is late.  Expected outputs validated with Postgres. */
    @Test
    public void minMaxNotMerged() {
        String sql = """
                CREATE TABLE T (ts INT, x INT LATENESS 2);
                CREATE VIEW V AS SELECT ts, MIN(x) AS mn, MAX(x) AS mx FROM T GROUP BY ts;""";
        String[] programs = {
                // As written
                sql,
                // Without LATENESS
                sql.replace(" LATENESS 2", "")
        };
        for (String program : programs) {
            var ccs = this.getCCS(program).compactAfterEachStep();
            ccs.step("INSERT INTO T VALUES(1, 5), (1, 6), (1, 7);", """
                     ts | mn | mx | weight
                    -----------------------
                     1  | 5  | 7  | 1""");
            ccs.step("INSERT INTO T VALUES(2, 100);", """
                     ts | mn  | mx  | weight
                    -------------------------
                     2  | 100 | 100 | 1""");
            // The waterline of x is 99; all values of group 1 are below it
            ccs.step("INSERT INTO T VALUES(2, 101);", """
                     ts | mn  | mx  | weight
                    -------------------------
                     2  | 100 | 100 | -1
                     2  | 100 | 101 | 1""");
            // Batches that change no output, so that compaction merges the traces
            for (int i = 0; i < 6; i++)
                ccs.step("INSERT INTO T VALUES(2, 100);", """
                         ts | mn | mx | weight
                        -----------------------""");
            ccs.step("INSERT INTO T VALUES(1, 99);", """
                     ts | mn | mx | weight
                    -----------------------
                     1  | 5  | 7  | -1
                     1  | 5  | 99 | 1""");
            // MAX needs 7, the largest value below the waterline
            ccs.step("REMOVE FROM T VALUES(1, 99);", """
                     ts | mn | mx | weight
                    -----------------------
                     1  | 5  | 99 | -1
                     1  | 5  | 7  | 1""");
        }
    }

    /** A row of T: time ts, with LATENESS, and a value v. */
    record TRow(int ts, int v) {}
    /** A row of S1: time ts, with LATENESS, and a value x. */
    record S1Row(int ts, int x) {}
    /** A row of S2: time ts, with a larger LATENESS, and a value x. */
    record S2Row(int ts, int x) {}
    /** A row of N: time ts, with LATENESS, that can be NULL, and a value x. */
    record NRow(Integer ts, int x) {}
    /** A row of U: a group key g, a value x with LATENESS, and a value y. */
    record URow(int g, int x, int y) {}
    /** A row of W: a group key k and a time ts with LATENESS. */
    record WRow(int k, int ts) {}
    /** A row of P: a key k and a value y; P has no LATENESS. */
    record PRow(int k, int y) {}
    /** A row of X: times t1 and t2 and a value v; X has no LATENESS. */
    record XRow(int t1, int t2, int v) {}
    /** A row of Y1: time t1, with LATENESS, time t2, and a value w. */
    record Y1Row(int t1, int t2, int w) {}
    /** A row of Y2: time t1, time t2, with LATENESS, and a value w. */
    record Y2Row(int t1, int t2, int w) {}

    static final int LATENESS = 10;

    static List<TablePair<?>> createTablePairs() {
        List<TablePair<?>> result = new ArrayList<>();
        result.add(new TablePair<>("T", false, TRow.class, Set.of("ts"), LATENESS));
        result.add(new TablePair<>("S1", false, S1Row.class, Set.of("ts"), LATENESS));
        result.add(new TablePair<>("S2", false, S2Row.class, Set.of("ts"), 6 * LATENESS));
        result.add(new TablePair<>("U", false, URow.class, Set.of("x"), LATENESS));
        result.add(new TablePair<>("W", false, WRow.class, Set.of("ts"), LATENESS));
        result.add(new TablePair<>("P", false, PRow.class, Set.of(), 0));
        result.add(new TablePair<>("X", false, XRow.class, Set.of(), 0));
        result.add(new TablePair<>("Y1", false, Y1Row.class, Set.of("t1"), LATENESS));
        result.add(new TablePair<>("Y2", false, Y2Row.class, Set.of("t2"), LATENESS));
        return result;
    }

    /** The columns of LATE_ALL and PLAIN_ALL: j names the view, a, b, c are its columns as VARCHAR. */
    static final List<String> COLUMNS = List.of("j", "a", "b", "c");

    /** The views over the tables with prefix {@code p}, two per shared integral:
     * - T joins S1 and S2 on ts: the two RetainKeys of T have different bounds and are merged
     *   into one that keeps a key when either keeps it;
     * - X joins Y1 and Y2 on (t1, t2): the two RetainKeys of X compare t1 and t2, and are merged
     *   into one that keeps a key when either keeps it;
     * - two views compute MAX(x) over U: the two RetainNValues of x have the same policy and
     *   one remains;
     * - MAX(ts) over W retains the latest ts of each key, but the join of W with P keeps every
     *   row of W, so the shared integral has no GC. */
    static List<String> views(String p) {
        return List.of(
                "SELECT 'join_s1' AS j, CAST(t.ts AS VARCHAR) AS a, CAST(t.v AS VARCHAR) AS b, " +
                        "CAST(s.x AS VARCHAR) AS c FROM " + p + "T t JOIN " + p + "S1 s ON t.ts = s.ts",
                "SELECT 'join_s2' AS j, CAST(t.ts AS VARCHAR) AS a, CAST(t.v AS VARCHAR) AS b, " +
                        "CAST(s.x AS VARCHAR) AS c FROM " + p + "T t JOIN " + p + "S2 s ON t.ts = s.ts",
                "SELECT 'max_x' AS j, CAST(g AS VARCHAR) AS a, CAST(MAX(x) AS VARCHAR) AS b, " +
                        "CAST(NULL AS VARCHAR) AS c FROM " + p + "U GROUP BY g",
                "SELECT 'max_x_sum_y' AS j, CAST(g AS VARCHAR) AS a, CAST(MAX(x) AS VARCHAR) AS b, " +
                        "CAST(SUM(y) AS VARCHAR) AS c FROM " + p + "U GROUP BY g",
                "SELECT 'max_ts' AS j, CAST(k AS VARCHAR) AS a, CAST(MAX(ts) AS VARCHAR) AS b, " +
                        "CAST(NULL AS VARCHAR) AS c FROM " + p + "W GROUP BY k",
                "SELECT 'join_p' AS j, CAST(w.k AS VARCHAR) AS a, CAST(w.ts AS VARCHAR) AS b, " +
                        "CAST(p.y AS VARCHAR) AS c FROM " + p + "W w JOIN " + p + "P p ON w.k = p.k",
                "SELECT 'join_y1' AS j, CAST(x.t1 AS VARCHAR) AS a, CAST(x.t2 AS VARCHAR) AS b, " +
                        "CAST(x.v AS VARCHAR) || ':' || CAST(y.w AS VARCHAR) AS c FROM " +
                        p + "X x JOIN " + p + "Y1 y ON x.t1 = y.t1 AND x.t2 = y.t2",
                "SELECT 'join_y2' AS j, CAST(x.t1 AS VARCHAR) AS a, CAST(x.t2 AS VARCHAR) AS b, " +
                        "CAST(x.v AS VARCHAR) || ':' || CAST(y.w AS VARCHAR) AS c FROM " +
                        p + "X x JOIN " + p + "Y2 y ON x.t1 = y.t1 AND x.t2 = y.t2");
    }

    /** The program: the tables of {@code pairs}, the views over the LATE_ tables and over the
     * PLAIN_ tables as LATE_ALL and PLAIN_ALL, and view D.
     * @param views  The views over the tables with a given prefix. */
    static String program(List<TablePair<?>> pairs, Function<String, List<String>> views) {
        StringBuilder sql = new StringBuilder();
        for (TablePair<?> pair : pairs) {
            sql.append(pair.create("LATE_", true));
            sql.append(pair.create("PLAIN_", false));
        }
        for (String prefix : TablePair.PREFIXES)
            sql.append("CREATE LOCAL VIEW ").append(prefix).append("ALL AS ")
                    .append(String.join("\nUNION ALL ", views.apply(prefix))).append(";\n");
        sql.append(DifferentialTester.differenceView(COLUMNS));
        return sql.toString();
    }

    /** The GC operators of the program: one RetainKeys for T, one for S1, one for S2, and one for
     * X; one RetainNValues for U; none for W. */
    @Test
    public void gcOperators() {
        CompilerCircuit cc = this.getCC(program(createTablePairs(), MergeGCIncrementalTests::views));
        CountGCOperators operators = new CountGCOperators(cc.compiler);
        cc.visit(operators);
        Assert.assertEquals("KKKKN", operators.kinds());
    }

    /** The views give the same output with and without LATENESS. */
    @Test
    public void sameOutputWithoutLateness() {
        List<TablePair<?>> pairs = createTablePairs();
        var tester = new DifferentialTester(this.getCCS(program(pairs, MergeGCIncrementalTests::views)), pairs, COLUMNS);
        tester.insert(new TRow(100, 1), new TRow(103, 2), new TRow(107, 3), new TRow(150, 4),
                new S1Row(100, 10), new S1Row(103, 11), new S2Row(100, 20), new S2Row(107, 21),
                new URow(1, 5, 1), new URow(1, 6, 1), new URow(2, 100, 1),
                new WRow(0, 100), new WRow(0, 105), new WRow(1, 100),
                new PRow(0, 7), new PRow(1, 8), new PRow(2, 9),
                new XRow(100, 100, 1), new XRow(100, 200, 2), new XRow(150, 100, 3), new XRow(195, 50, 4),
                new Y1Row(100, 100, 10), new Y2Row(100, 100, 20));
        // The waterlines move: ts of T and of S1 to 190, ts of S2 to 140, x of U to 91, ts of W to 190,
        // t1 of Y1 and t2 of Y2 to 190
        tester.insert(new TRow(200, 5), new S1Row(200, 12), new S2Row(200, 22), new URow(2, 101, 1), new WRow(0, 200),
                new Y1Row(200, 0, 11), new Y2Row(0, 200, 21));
        // Batches of X that change no output, so that compaction merges the first batch of X
        for (int i = 0; i < 6; i++)
            tester.insert(Set.of("X"), new XRow(300 + i, 300, 0));
        // Rows of X that only one of the two policies keeps: t2 at the waterline of Y2, t1 at the waterline of Y1
        tester.insert(Set.of("Y1", "Y2"), new Y2Row(100, 200, 22), new Y1Row(195, 50, 12));
        // A row of S2 below the waterline of S1 joins a row of T that the merged bound must keep
        tester.insert(Set.of("S2"), new S2Row(150, 23));
        // Rows at the waterlines of S1 and of S2
        tester.insert(Set.of("S1", "S2"), new S1Row(190, 13), new S2Row(140, 24));
        // Late rows, which the tester skips
        tester.insert(new TRow(145, 6), new S2Row(130, 25), new Y1Row(150, 100, 13), new Y2Row(100, 150, 23));
        // Retract the joins at the waterline
        tester.delete(Set.of("T"), new TRow(200, 5));
        tester.delete(Set.of("X"), new XRow(100, 200, 2));
        // MAX(x) of group 1 of U rises above the waterline; then a change of y only
        tester.insert(Set.of("U"), new URow(1, 100, 2));
        tester.insert(Set.of("U"), new URow(2, 103, 5));
        // A new row of P: the join emits every row of W with that key, the old ones included
        tester.insert(Set.of("P"), new PRow(0, 70));
        tester.delete(Set.of("P"), new PRow(1, 8));
        // The latest row of group 0 of W goes: MAX(ts) falls back to a row below the waterline
        tester.delete(Set.of("W"), new WRow(0, 200));
        tester.insert(Set.of("W"), new WRow(1, 195), new WRow(2, 191));
        // The waterlines move again; changes after the second compaction
        tester.insert(new TRow(300, 7), new S1Row(300, 14), new S2Row(300, 26), new URow(3, 200, 1), new WRow(2, 300));
        tester.insert(Set.of("T"), new TRow(295, 8));
        tester.insert(Set.of("S2"), new S2Row(295, 27));
        tester.delete(Set.of("U"), new URow(3, 200, 1));
    }

    /** T joins N, the projection of N to ts, and S1 on ts.  The bounds of the two joins with N,
     * whose ts can be NULL, have different types, so the merged RetainKeys of T combines its
     * three bounds with a chain of Apply2 operators. */
    static List<String> boundTypesViews(String p) {
        return List.of(
                "SELECT 'join_n' AS j, CAST(t.ts AS VARCHAR) AS a, CAST(t.v AS VARCHAR) AS b, " +
                        "CAST(n.x AS VARCHAR) AS c FROM " + p + "T t JOIN " + p + "N n ON t.ts = n.ts",
                "SELECT 'join_n_ts' AS j, CAST(t.ts AS VARCHAR) AS a, CAST(t.v AS VARCHAR) AS b, " +
                        "CAST(NULL AS VARCHAR) AS c FROM " + p + "T t JOIN (SELECT ts FROM " + p + "N) n ON t.ts = n.ts",
                "SELECT 'join_s1' AS j, CAST(t.ts AS VARCHAR) AS a, CAST(t.v AS VARCHAR) AS b, " +
                        "CAST(s.x AS VARCHAR) AS c FROM " + p + "T t JOIN " + p + "S1 s ON t.ts = s.ts");
    }

    /** The joins of {@link #boundTypesViews} give the same output with and without LATENESS. */
    @Test
    public void differentBoundTypesSameOutputWithoutLateness() {
        List<TablePair<?>> pairs = List.of(
                new TablePair<>("T", false, TRow.class, Set.of("ts"), LATENESS),
                new TablePair<>("N", true, NRow.class, Set.of("ts"), LATENESS),
                new TablePair<>("S1", false, S1Row.class, Set.of("ts"), LATENESS));
        String sql = program(pairs, MergeGCIncrementalTests::boundTypesViews);
        var tester = new DifferentialTester(this.getCCS(sql), pairs, COLUMNS);
        tester.insert(new TRow(100, 1), new TRow(103, 2), new TRow(150, 3),
                new NRow(100, 10), new NRow(103, 11), new NRow(null, 12), new S1Row(100, 20));
        // The waterline of N moves to 190; the waterline of S1 stays at 90
        tester.insert(Set.of("N"), new NRow(200, 13));
        // Batches of N that change no output, so that compaction applies the bounds to T
        for (int i = 0; i < 6; i++)
            tester.insert(Set.of("N"), new NRow(300 + i, 0));
        // Rows of S1 above its waterline join rows of T below the waterline of N, which only the
        // bound from S1 keeps
        tester.insert(Set.of("S1"), new S1Row(103, 21), new S1Row(150, 22));
        tester.insert(Set.of("N"), new NRow(null, 14));
    }
}
