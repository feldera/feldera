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

    /** The two retain operators of T have different bounds; they are merged 
     * into one operator with the smaller bound. */
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
    /** A row of U: a group key g, a value x with LATENESS, and a value y. */
    record URow(int g, int x, int y) {}
    /** A row of W: a group key k and a time ts with LATENESS. */
    record WRow(int k, int ts) {}
    /** A row of P: a key k and a value y; P has no LATENESS. */
    record PRow(int k, int y) {}

    static final int LATENESS = 10;

    static List<TablePair<?>> createTablePairs() {
        List<TablePair<?>> result = new ArrayList<>();
        result.add(new TablePair<>("T", false, TRow.class, Set.of("ts"), LATENESS));
        result.add(new TablePair<>("S1", false, S1Row.class, Set.of("ts"), LATENESS));
        result.add(new TablePair<>("S2", false, S2Row.class, Set.of("ts"), 6 * LATENESS));
        result.add(new TablePair<>("U", false, URow.class, Set.of("x"), LATENESS));
        result.add(new TablePair<>("W", false, WRow.class, Set.of("ts"), LATENESS));
        result.add(new TablePair<>("P", false, PRow.class, Set.of(), 0));
        return result;
    }

    /** The columns of LATE_ALL and PLAIN_ALL: j names the view, a, b, c are its columns as VARCHAR. */
    static final List<String> COLUMNS = List.of("j", "a", "b", "c");

    /** The views over the tables with prefix {@code p}, two per shared integral:
     * - T joins S1 and S2 on ts: the two RetainKeys of T have different bounds and are merged
     *   into one with the smaller bound;
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
                        "CAST(p.y AS VARCHAR) AS c FROM " + p + "W w JOIN " + p + "P p ON w.k = p.k");
    }

    /** The program: the tables of {@code pairs}, the views over the LATE_ tables and over the
     * PLAIN_ tables as LATE_ALL and PLAIN_ALL, and view D. */
    static String program(List<TablePair<?>> pairs) {
        StringBuilder sql = new StringBuilder();
        for (TablePair<?> pair : pairs) {
            sql.append(pair.create("LATE_", true));
            sql.append(pair.create("PLAIN_", false));
        }
        for (String prefix : TablePair.PREFIXES)
            sql.append("CREATE LOCAL VIEW ").append(prefix).append("ALL AS ")
                    .append(String.join("\nUNION ALL ", views(prefix))).append(";\n");
        sql.append(DifferentialTester.differenceView(COLUMNS));
        return sql.toString();
    }

    /** The GC operators of the program: one RetainKeys for T, one for S1, and one for S2; one
     * RetainNValues for U; none for W. */
    @Test
    public void gcOperators() {
        CompilerCircuit cc = this.getCC(program(createTablePairs()));
        CountGCOperators operators = new CountGCOperators(cc.compiler);
        cc.visit(operators);
        Assert.assertEquals("KKKN", operators.kinds());
    }

    /** The views give the same output with and without LATENESS. */
    @Test
    public void sameOutputWithoutLateness() {
        List<TablePair<?>> pairs = createTablePairs();
        var tester = new DifferentialTester(this.getCCS(program(pairs)), pairs, COLUMNS);
        tester.insert(new TRow(100, 1), new TRow(103, 2), new TRow(107, 3), new TRow(150, 4),
                new S1Row(100, 10), new S1Row(103, 11), new S2Row(100, 20), new S2Row(107, 21),
                new URow(1, 5, 1), new URow(1, 6, 1), new URow(2, 100, 1),
                new WRow(0, 100), new WRow(0, 105), new WRow(1, 100),
                new PRow(0, 7), new PRow(1, 8), new PRow(2, 9));
        // The waterlines move: ts of T and of S1 to 190, ts of S2 to 140, x of U to 91, ts of W to 190
        tester.insert(new TRow(200, 5), new S1Row(200, 12), new S2Row(200, 22), new URow(2, 101, 1), new WRow(0, 200));
        // A row of S2 below the waterline of S1 joins a row of T that the merged bound must keep
        tester.insert(Set.of("S2"), new S2Row(150, 23));
        // Rows at the waterlines of S1 and of S2
        tester.insert(Set.of("S1", "S2"), new S1Row(190, 13), new S2Row(140, 24));
        // Late rows, which the tester skips
        tester.insert(new TRow(145, 6), new S2Row(130, 25));
        // Retract the joins at the waterline
        tester.delete(Set.of("T"), new TRow(200, 5));
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
}
