package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.operator.DBSPIntegrateTraceRetainNValuesOperator;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.IntegerColumn;
import org.dbsp.sqlCompiler.compiler.sql.tools.LatenessModel;
import org.dbsp.sqlCompiler.compiler.sql.tools.LatenessSchema;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

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
    record Row(int k, @Nullable Integer g, @Nullable Integer c, @Nullable Integer p) {
        String values() {
            return "(" + this.k + ", " + this.g + ", " + this.c + ", " + this.p + ")";
        }

        /** True if some column of the row is NULL. */
        boolean hasNull() {
            return this.g == null || this.c == null || this.p == null;
        }
    }

    /** A pair of tables with the same columns, one with LATENESS on some columns and one
     * without; every change goes to both or to neither.  The schema of the tables is
     * <pre>
     * CREATE TABLE LATE_name (k INT NOT NULL, g INT, c INT, p INT);
     * CREATE TABLE PLAIN_name (k INT NOT NULL, g INT, c INT, p INT);
     * </pre>
     * where k is a group key, g a group key that may have LATENESS, c the compared value,
     * and p the payload; g, c, and p are NOT NULL unless the pair is nullable.
     *
     * <p>The model tracks the waterlines of the LATE_ table, so that a change is applied to the
     * pair only if it is not late. */
    static final class TablePair {
        /** The LATENESS of the columns that have one. */
        static final int LATENESS = 10;
        /** The prefixes of the names of the two tables. */
        static final String[] PREFIXES = { "LATE_", "PLAIN_" };
        final String name;
        final boolean nullable;
        /** The columns of the LATE_ table that have LATENESS. */
        final LatenessSchema<Row> schema;
        final LatenessModel<Row> model;

        /** @param lateG  True if column 'g' of the LATE_ table has LATENESS. */
        TablePair(String name, boolean nullable, boolean lateG, boolean lateC, boolean lateP) {
            this.name = name;
            this.nullable = nullable;
            this.schema = new LatenessSchema<>();
            if (lateG)
                this.schema.addColumn(new IntegerColumn<>("g", Row::g, LATENESS));
            if (lateC)
                this.schema.addColumn(new IntegerColumn<>("c", Row::c, LATENESS));
            if (lateP)
                this.schema.addColumn(new IntegerColumn<>("p", Row::p, LATENESS));
            this.model = new LatenessModel<>(this.schema);
        }

        /** The CREATE TABLE statement for one table of the pair.
         * @param prefix    Prefix of the table name, LATE_ or PLAIN_.
         * @param lateness  True to add LATENESS to the columns in the schema. */
        String create(String prefix, boolean lateness) {
            String notNull = this.nullable ? "" : " NOT NULL";
            Function<String, String> column = name -> ", " + name + " INT" + notNull +
                    (lateness ? this.schema.lateness(name) : "");
            return "CREATE TABLE " + prefix + this.name + " (k INT NOT NULL" +
                    column.apply("g") + column.apply("c") + column.apply("p") + ");\n";
        }

        /** True if the row can be stored in the tables: a NULL needs a nullable pair. */
        boolean fits(Row row) {
            return !row.hasNull() || this.nullable;
        }

        /** The statements inserting {@code row} into both tables, or an empty string if the row
         * does not fit or is late. */
        String insert(Row row) {
            if (!this.fits(row) || !this.model.insert(row))
                return "";
            return this.statements("INSERT INTO ", row);
        }

        /** The statements deleting {@code row} from both tables, or an empty string if the row
         * does not fit, is late, or is absent. */
        String delete(Row row) {
            if (!this.fits(row) || !this.model.delete(row))
                return "";
            return this.statements("REMOVE FROM ", row);
        }

        /** The statement {@code command} applied to {@code row}, once for each table. */
        String statements(String command, Row row) {
            StringBuilder result = new StringBuilder();
            for (String prefix : PREFIXES)
                result.append(command).append(prefix).append(this.name)
                        .append(" VALUES ").append(row.values()).append(";\n");
            return result.toString();
        }
    }

    /** One pair per placement of the LATENESS columns and nullability of the compared column. */
    static List<TablePair> createTablePairs() {
        List<TablePair> result = new ArrayList<>();
        for (boolean nullable : new boolean[] { false, true }) {
            for (boolean lateC : new boolean[] { false, true }) {
                for (boolean lateP : new boolean[] { false, true }) {
                    // When neither c nor p has LATENESS, the group key g has it
                    boolean lateG = !lateC && !lateP;
                    String name = (lateG ? "G" : "") + (lateC ? "C" : "") + (lateP ? "P" : "") +
                            (nullable ? "_L" : "_N");
                    result.add(new TablePair(name, nullable, lateG, lateC, lateP));
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

    /* The generated program, for one table pair and one aggregate:
     * CREATE TABLE LATE_P_L (k INT NOT NULL, g INT, c INT, p INT LATENESS 10);
     * CREATE TABLE PLAIN_P_L (k INT NOT NULL, g INT, c INT, p INT);
     * CREATE LOCAL VIEW LATE_ALL AS ...
     * UNION ALL SELECT 'P_L_argmin_' AS agg, CAST(NULL AS VARCHAR) AS gkey, CAST(ARG_MIN(p, c) AS VARCHAR) AS r1 FROM LATE_P_L
     * UNION ALL ...;
     * CREATE LOCAL VIEW PLAIN_ALL AS ...
     * UNION ALL SELECT 'P_L_argmin_' AS agg, CAST(NULL AS VARCHAR) AS gkey, CAST(ARG_MIN(p, c) AS VARCHAR) AS r1 FROM PLAIN_P_L
     * UNION ALL ...;
     * CREATE VIEW D AS
     * SELECT 'late' AS side, * FROM (SELECT * FROM LATE_ALL EXCEPT ALL SELECT * FROM PLAIN_ALL) late_extra
     * UNION ALL
     * SELECT 'plain' AS side, * FROM (SELECT * FROM PLAIN_ALL EXCEPT ALL SELECT * FROM LATE_ALL) plain_extra
     * UNION ALL
     * SELECT 'late row' AS side, table_or_view_name AS agg, message AS gkey, metadata AS r1 FROM ERROR_VIEW;
     */
    static String differentialProgram(List<TablePair> pairs, List<Aggregate> aggregates) {
        StringBuilder sql = new StringBuilder();
        for (TablePair pair : pairs) {
            sql.append(pair.create("LATE_", true));
            sql.append(pair.create("PLAIN_", false));
        }
        for (String prefix : TablePair.PREFIXES) {
            List<String> branches = new ArrayList<>();
            for (TablePair pair : pairs) {
                // Grouping by g makes the key monotone only when g has LATENESS
                String[] groupings = pair.schema.hasLateness("g") ? new String[] { "g" } : new String[] { "", "k" };
                for (String grouping : groupings) {
                    for (Aggregate aggregate : aggregates) {
                        String key = grouping.isEmpty() ? "NULL" : grouping;
                        String groupBy = grouping.isEmpty() ? "" : " GROUP BY " + grouping;
                        // The casts remove the waterlines, so that the difference below keeps all its state
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
        // D is the symmetric difference of LATE_ALL and PLAIN_ALL, plus the rows that the
        // circuit reports as late, which the tests never expect
        StringBuilder unusedResults = new StringBuilder();
        for (int i = 2; i <= aggregates.get(0).results.length; i++)
            unusedResults.append(", NULL AS r").append(i);
        sql.append("""
                CREATE VIEW D AS
                SELECT 'late' AS side, * FROM (SELECT * FROM LATE_ALL EXCEPT ALL SELECT * FROM PLAIN_ALL) late_extra
                UNION ALL
                SELECT 'plain' AS side, * FROM (SELECT * FROM PLAIN_ALL EXCEPT ALL SELECT * FROM LATE_ALL) plain_extra
                UNION ALL
                SELECT 'late row' AS side, table_or_view_name AS agg, message AS gkey, metadata AS r1""")
                .append(unusedResults).append(" FROM ERROR_VIEW;");
        return sql.toString();
    }

    /** Applies changes to every table pair and checks after each step that the LATE_ and
     * PLAIN_ tables produce the same outputs. */
    static final class DifferentialTester {
        final CompilerCircuitStream ccs;
        final List<TablePair> pairs;
        /** The output of view D when the two sides agree: its header and no rows. */
        final String empty;

        /** @param resultColumns  Number of result columns of each aggregate view. */
        DifferentialTester(CompilerCircuitStream ccs, List<TablePair> pairs, int resultColumns) {
            this.ccs = ccs;
            this.pairs = pairs;
            StringBuilder header = new StringBuilder(" side | agg | gkey");
            for (int i = 1; i <= resultColumns; i++)
                header.append(" | r").append(i);
            this.empty = header.append(" | weight\n---").toString();
        }

        /** Insert {@code rows} in one step. */
        void insert(Row... rows) {
            this.step(Set.of(), List.of(rows), List.of());
        }

        /** Insert {@code rows} in one step, which must reach at least the pairs named {@code mustReach}. */
        void insert(Set<String> mustReach, Row... rows) {
            this.step(mustReach, List.of(rows), List.of());
        }

        /** Delete {@code rows} in one step. */
        void delete(Row... rows) {
            this.step(Set.of(), List.of(), List.of(rows));
        }

        /** Delete {@code rows} in one step, which must reach at least the pairs named {@code mustReach}. */
        void delete(Set<String> mustReach, Row... rows) {
            this.step(mustReach, List.of(), List.of(rows));
        }

        /** Move every waterline up with a row of group 2 at time {@code t}. */
        void advanceTime(int t) {
            this.insert(row(2, t, t));
        }

        void step(Set<String> mustReach, List<Row> inserts, List<Row> deletes) {
            StringBuilder sql = new StringBuilder();
            Set<String> reached = new HashSet<>();
            for (TablePair pair : this.pairs) {
                StringBuilder changes = new StringBuilder();
                for (Row row : inserts)
                    changes.append(pair.insert(row));
                for (Row row : deletes)
                    changes.append(pair.delete(row));
                pair.model.commit();
                if (!changes.isEmpty())
                    reached.add(pair.name);
                sql.append(changes);
            }
            Assert.assertTrue("Reached " + reached + ", expected " + mustReach, reached.containsAll(mustReach));
            this.ccs.step(sql.toString(), this.empty);
        }
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

    /** Differential test: the tables without LATENESS are an oracle for the tables with LATENESS.
     * <ul>
     *   <li>Each LATE_ table has LATENESS on some columns; its PLAIN_ twin has the same
     *       columns without LATENESS.</li>
     *   <li>Both tables receive the same changes; a change that is late for the LATE_ table
     *       goes to neither.</li>
     *   <li>LATENESS only enables GC, so without late changes both sides must produce the
     *       same outputs.</li>
     *   <li>The circuit compacts after each step, so GC discards state on the LATE_ side.</li>
     *   <li>View D is the symmetric difference of the two sides, plus the rows that the
     *       circuit reports as late in ERROR_VIEW; every step expects D to stay empty.</li>
     *   <li>So the tests also assert that the circuit never receives late data: the model and
     *       the circuit agree on the waterlines.</li>
     * </ul>
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
        List<TablePair> pairs = createTablePairs();
        var ccs = this.getCCS(differentialProgram(pairs, aggregates)).compactAfterEachStep();
        return new DifferentialTester(ccs, pairs, aggregates.get(0).results.length);
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
        // Move every waterline to 140; the results of groups 0, 1, and 3 are now below it
        tester.advanceTime(150);
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
        tester.advanceTime(190);
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
