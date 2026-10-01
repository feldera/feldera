package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainNValuesOperator;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.DifferentialTester;
import org.dbsp.sqlCompiler.compiler.sql.tools.TablePair;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** GC of MIN, MAX, ARG_MIN, and ARG_MAX inputs. */
public class ArgMinGCIncrementalTests extends StreamingTestBase {
    /** Rows with a NULL compared value below the waterline must not displace the row
     * that ARG_MIN returns.  The program runs with and without LATENESS; no input row is late.
     * Expected outputs validated with Postgres. */
    @Test
    public void nullComparedBelowWaterline() {
        String sql = """
                CREATE TABLE T (ts INT LATENESS 2, x INT);
                CREATE VIEW V AS SELECT ARG_MIN(ts, x) AS a FROM T;""";
        String[] programs = {
                // As written
                sql,
                // Without LATENESS
                sql.replace(" LATENESS 2", "")
        };
        for (String program : programs) {
            var ccs = this.getCCS(program).compactAfterEachStep();
            ccs.step("INSERT INTO T VALUES (1, 5), (2, NULL), (3, NULL);", """
                     a | weight
                    ------------
                     1 | 1""");
            // Move the waterline past the first rows; x is NULL, so ARG_MIN does not change
            ccs.step("INSERT INTO T VALUES (100, NULL);", """
                     a | weight
                    ------------""");
            ccs.step("INSERT INTO T VALUES (150, NULL);", """
                     a | weight
                    ------------""");
            // 7 is larger than 5
            ccs.step("INSERT INTO T VALUES (200, 7);", """
                     a | weight
                    ------------""");
        }
    }

    /** A row of the tables: group key k, group key g, compared value c, payload p. */
    record Row(int k, @Nullable Integer g, @Nullable Integer c, @Nullable Integer p) {}

    /** The LATENESS of the columns that have one. */
    static final int LATENESS = 10;

    /** One pair per placement of the LATENESS columns and nullability of the columns.
     * The schema of the tables is
     * <pre>
     * CREATE TABLE LATE_name (k INT NOT NULL, g INT, c INT, p INT);
     * CREATE TABLE PLAIN_name (k INT NOT NULL, g INT, c INT, p INT);
     * </pre>
     * where k is a group key, g a group key that may have LATENESS, c the compared value,
     * and p the payload; g, c, and p are NOT NULL unless the pair is nullable. */
    static List<TablePair<Row>> createTablePairs() {
        List<TablePair<Row>> result = new ArrayList<>();
        for (boolean nullable : new boolean[] { false, true }) {
            for (boolean lateC : new boolean[] { false, true }) {
                for (boolean lateP : new boolean[] { false, true }) {
                    // When neither c nor p has LATENESS, the group key g has it
                    boolean lateG = !lateC && !lateP;
                    String name = (lateG ? "G" : "") + (lateC ? "C" : "") + (lateP ? "P" : "") +
                            (nullable ? "_L" : "_N");
                    Set<String> late = new HashSet<>();
                    if (lateG)
                        late.add("g");
                    if (lateC)
                        late.add("c");
                    if (lateP)
                        late.add("p");
                    result.add(new TablePair<>(name, nullable, Row.class, late, LATENESS));
                }
            }
        }
        return result;
    }

    /** An aggregate view: its name and the expressions of its result columns r1, r2, ... */
    record Aggregate(String name, String... results) {}

    /** Views with a single aggregate. */
    static final List<Aggregate> SINGLE_AGGREGATES = List.of(
            new Aggregate("min", "MIN(c)"),
            new Aggregate("max", "MAX(c)"),
            new Aggregate("argmin", "ARG_MIN(p, c)"),
            new Aggregate("argmax", "ARG_MAX(p, c)"));

    /** A view with MIN and MAX of the same input, which keep opposite ends of the values
     * below the waterline. */
    static final List<Aggregate> MIN_AND_MAX = List.of(new Aggregate("minmax", "MIN(c)", "MAX(c)"));

    /** The columns of LATE_ALL and PLAIN_ALL: the name of the view, its group key, and its results. */
    static List<String> columns(List<Aggregate> aggregates) {
        List<String> result = new ArrayList<>(List.of("agg", "gkey"));
        for (int i = 1; i <= aggregates.get(0).results.length; i++)
            result.add("r" + i);
        return result;
    }

    /* The generated program, for one table pair and one aggregate:
     * CREATE TABLE LATE_P_L (k INT NOT NULL, g INT, c INT, p INT LATENESS 10);
     * CREATE TABLE PLAIN_P_L (k INT NOT NULL, g INT, c INT, p INT);
     * CREATE LOCAL VIEW LATE_ALL AS ...
     * UNION ALL SELECT 'P_L_argmin_' AS agg, CAST(NULL AS VARCHAR) AS gkey, CAST(ARG_MIN(p, c) AS VARCHAR) AS r1 FROM LATE_P_L
     * UNION ALL ...;
     * CREATE LOCAL VIEW PLAIN_ALL AS ...
     * UNION ALL SELECT 'P_L_argmin_' AS agg, CAST(NULL AS VARCHAR) AS gkey, CAST(ARG_MIN(p, c) AS VARCHAR) AS r1 FROM PLAIN_P_L
     * UNION ALL ...;
     * CREATE VIEW D AS ...;
     */
    static String differentialProgram(List<TablePair<Row>> pairs, List<Aggregate> aggregates) {
        StringBuilder sql = new StringBuilder();
        for (TablePair<Row> pair : pairs) {
            sql.append(pair.create("LATE_", true));
            sql.append(pair.create("PLAIN_", false));
        }
        for (String prefix : TablePair.PREFIXES) {
            List<String> branches = new ArrayList<>();
            for (TablePair<Row> pair : pairs) {
                // Grouping by g makes the key monotone only when g has LATENESS
                String[] groupings = pair.schema.hasLateness("g") ? new String[] { "g" } : new String[] { "", "k" };
                for (String grouping : groupings) {
                    for (Aggregate aggregate : aggregates) {
                        String key = grouping.isEmpty() ? "NULL" : grouping;
                        String groupBy = grouping.isEmpty() ? "" : " GROUP BY " + grouping;
                        // The casts remove the waterlines, so that GC does not prune the traces of the operators
                        // that compute view D, which compares the two sides
                        StringBuilder branch = new StringBuilder("SELECT '" + pair.name + "_" + aggregate.name +
                                "_" + grouping + "' AS agg, CAST(" + key + " AS VARCHAR) AS gkey");
                        for (int i = 0; i < aggregate.results.length; i++)
                            branch.append(", CAST(").append(aggregate.results[i]).append(" AS VARCHAR) AS r").append(i + 1);
                        branch.append(" FROM ").append(prefix).append(pair.name).append(groupBy);
                        branches.add(branch.toString());
                    }
                }
            }
            sql.append("CREATE LOCAL VIEW ").append(prefix).append("ALL AS ")
                    .append(String.join("\nUNION ALL ", branches)).append(";\n");
        }
        sql.append(DifferentialTester.differenceView(columns(aggregates)));
        return sql.toString();
    }

    /** A row whose group key g and payload p both hold the time t. */
    static Row row(int k, int t, @Nullable Integer c) {
        return new Row(k, t, c, t);
    }

    /** A row with a NULL group key g. */
    static Row nullKey(int k, int c, int p) {
        return new Row(k, null, c, p);
    }

    /** A row with a NULL payload p; the group key g holds the time t. */
    static Row nullPayload(int k, int t, int c) {
        return new Row(k, t, c, null);
    }

    /** The tables without LATENESS are an oracle for the tables with LATENESS.
     * The views cover MIN, MAX, ARG_MIN, and ARG_MAX, every grouping, every placement of the
     * LATENESS columns, and the nullability of the columns. */
    @Test
    public void sameOutputWithoutLateness() {
        runSteps(this.differentialTester(SINGLE_AGGREGATES));
    }

    /** The differential steps on a view with MIN and MAX of the same input. */
    @Test
    public void minAndMaxOfOneInput() {
        runSteps(this.differentialTester(MIN_AND_MAX));
    }

    /** A differential program with the views of {@code aggregates} over every table pair. */
    DifferentialTester differentialTester(List<Aggregate> aggregates) {
        List<TablePair<Row>> pairs = createTablePairs();
        var ccs = this.getCCS(differentialProgram(pairs, aggregates));
        return new DifferentialTester(ccs, pairs, columns(aggregates));
    }

    /** The steps move the result of every group below the waterline, where only the rows
     * that GC keeps can produce it, and then apply each kind of change that can still reach
     * the group; the comment on each step names the case.  All pairs receive the same steps;
     * each pair skips the changes that are late for its LATE_ table.  The waterlines advance
     * in two phases, so the rows that GC keeps must survive more than one compaction. */
    static void runSteps(DifferentialTester tester) {
        // One step, before any waterline exists, so that no row is late
        tester.insert(
                // Group 0: minimum 103 tied at payloads 100 and 104, three NULLs, maximum 108 tied at payloads 105 and 110
                row(0, 100, 103), row(0, 101, null), row(0, 102, null), row(0, 103, null),
                row(0, 104, 103), row(0, 105, 108), row(0, 110, 108),
                // Group 1: minimum 104
                row(1, 106, 110), row(1, 107, 104),
                // Group 3: a single value and a NULL
                row(3, 108, 120), row(3, 109, null),
                // Group 4: only NULLs
                row(4, 111, null), row(4, 112, null),
                // Group 5: the minimum 90 has a NULL payload
                nullPayload(5, 114, 90), row(5, 115, 95),
                // Group 6: a NULL group key g
                nullKey(6, 130, 116));
        // Group 2 carries the time: move every waterline to 140; the results of groups 0, 1,
        // and 3 are now below it
        tester.insert(row(2, 150, 150));
        // A NULL compared value in group g = 140, exactly at the waterline
        tester.insert(Set.of("G_L"), row(0, 140, null));
        // An insert that leaves the minimum of group 0 unchanged
        tester.insert(row(0, 141, 150));
        // A new maximum for group 0, then back to the maximum below the waterline
        tester.insert(row(0, 142, 160));
        tester.delete(row(0, 142, 160));
        // A new minimum for group 1 where c has no LATENESS, then back to the minimum below the waterline
        tester.insert(Set.of("P_N", "P_L"), row(1, 143, 50));
        tester.delete(Set.of("P_N", "P_L"), row(1, 143, 50));
        // A tie with the only value of group 3, where c has no LATENESS
        tester.insert(Set.of("P_N", "P_L"), row(3, 144, 120));
        // Delete the NULL; g = 140 is still at the waterline, so the change is not late
        tester.delete(Set.of("G_L"), row(0, 140, null));
        // The first non-NULL value of group 4, above the waterline of c
        tester.insert(Set.of("C_L", "CP_L"), row(4, 145, 155));
        // A new minimum for the NULL group key; a NULL g is never late
        tester.insert(Set.of("G_L"), nullKey(6, 125, 146));
        // An old row with a NULL compared value; late unless only c has LATENESS
        tester.delete(Set.of("C_L"), row(0, 101, null));
        // The old minimum of group 5, whose payload is NULL; late unless only p has LATENESS
        tester.delete(Set.of("P_L"), nullPayload(5, 114, 90));
        // Move every waterline to 180, then a new maximum for group 0 after the second compaction
        tester.insert(row(2, 190, 190));
        tester.insert(row(0, 181, 200));
    }

    /** With LATENESS on the compared column, MIN, MAX, ARG_MIN, and ARG_MAX each retain their
     * input with one operator, whether the compared column is nullable or not. */
    @Test
    public void oneRetainPerAggregate() {
        for (String aggregate : new String[] { "MIN(x)", "MAX(x)", "ARG_MIN(z, x)", "ARG_MAX(z, x)" }) {
            for (boolean nullable : new boolean[] { false, true }) {
                var cc = this.getCC("CREATE TABLE T(ts INT, z VARCHAR, x INT" + (nullable ? "" : " NOT NULL") +
                        " LATENESS 2);\nCREATE VIEW V AS SELECT ts, " + aggregate + " FROM T GROUP BY ts;");
                int[] retains = new int[1];
                cc.visit(new CircuitVisitor(cc.compiler) {
                    @Override
                    public void postorder(DBSPIntegrateTraceRetainNValuesOperator operator) {
                        retains[0]++;
                    }
                });
                Assert.assertEquals(aggregate + (nullable ? " nullable" : ""), 1, retains[0]);
            }
        }
    }
}
