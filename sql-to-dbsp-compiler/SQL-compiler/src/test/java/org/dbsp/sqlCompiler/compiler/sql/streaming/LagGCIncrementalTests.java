package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.operator.DBSPLagOperator;
import org.dbsp.sqlCompiler.circuit.operator.DBSPLagWithWaterlineOperator;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuit;
import org.dbsp.sqlCompiler.compiler.sql.tools.DifferentialTester;
import org.dbsp.sqlCompiler.compiler.sql.tools.TablePair;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** GC of the state of LAG and LEAD.  When the first column of the sort order has a waterline,
 * is ascending, and sorts its NULL values last, the operator keeps the rows at or above the
 * waterline and a few rows below it in each partition. */
public class LagGCIncrementalTests extends StreamingTestBase {
    /** LEAD over two copies of a row, which lead to different rows.  A row at the waterline then
     * changes the lead of the second copy, and the operator retracts the outputs of both copies.
     * Expected outputs validated with Postgres. */
    @Test
    public void leadOfCopies() {
        String sql = """
                CREATE TABLE T (ts BIGINT NOT NULL LATENESS 10, x BIGINT NOT NULL);
                CREATE VIEW V AS SELECT ts, x, LEAD(x) OVER (ORDER BY ts) AS n FROM T;""";
        String[] programs = {
                // As written
                sql,
                // Without LATENESS
                sql.replace(" LATENESS 10", "")
        };
        for (String program : programs) {
            var ccs = this.getCCS(program).compactAfterEachStep();
            ccs.step("INSERT INTO T VALUES(50, 1), (50, 1);", """
                     ts | x | n | weight
                    ---------------------
                     50 | 1 | 1 | 1
                     50 | 1 |   | 1""");
            ccs.step("INSERT INTO T VALUES(100, 0);", """
                     ts  | x | n | weight
                    ----------------------
                     50  | 1 |   | -1
                     50  | 1 | 0 | 1
                     100 | 0 |   | 1""");
            // Move the waterline of ts to 95, one row per step, so that compaction applies GC
            for (int ts = 101; ts < 106; ts++)
                ccs.step("INSERT INTO T VALUES(" + ts + ", 0);", """
                         ts   | x | n | weight
                        -----------------------
                         PREV | 0 |   | -1
                         PREV | 0 | 0 | 1
                         TS   | 0 |   | 1"""
                        .replace("PREV", Integer.toString(ts - 1))
                        .replace("TS", Integer.toString(ts)));
            // Not late; the lead of the second copy of 50 becomes 95
            ccs.step("INSERT INTO T VALUES(95, 7);", """
                     ts | x | n | weight
                    ---------------------
                     50 | 1 | 0 | -1
                     50 | 1 | 7 | 1
                     95 | 7 | 0 | 1""");
        }
    }

    /** LAG(2) of rows at the waterline reads the two rows below it.  Expected outputs validated
     * with Postgres. */
    @Test
    public void lagAtWaterline() {
        String sql = """
                CREATE TABLE T (ts BIGINT NOT NULL LATENESS 10, x BIGINT NOT NULL);
                CREATE VIEW V AS SELECT ts, x, LAG(x, 2) OVER (ORDER BY ts) AS p FROM T;""";
        String[] programs = {
                // As written
                sql,
                // Without LATENESS
                sql.replace(" LATENESS 10", "")
        };
        for (String program : programs) {
            var ccs = this.getCCS(program).compactAfterEachStep();
            ccs.step("INSERT INTO T VALUES(50, 1), (60, 2), (70, 3);", """
                     ts | x | p | weight
                    ---------------------
                     50 | 1 |   | 1
                     60 | 2 |   | 1
                     70 | 3 | 1 | 1""");
            ccs.step("INSERT INTO T VALUES(100, 0);", """
                     ts  | x | p | weight
                    ----------------------
                     100 | 0 | 2 | 1""");
            ccs.step("INSERT INTO T VALUES(101, 0);", """
                     ts  | x | p | weight
                    ----------------------
                     101 | 0 | 3 | 1""");
            // Move the waterline of ts to 95, one row per step, so that compaction applies GC
            for (int ts = 102; ts < 106; ts++)
                ccs.step("INSERT INTO T VALUES(" + ts + ", 0);", """
                         ts | x | p | weight
                        ---------------------
                         TS | 0 | 0 | 1""".replace("TS", Integer.toString(ts)));
            // Not late; the rows at 100 and 101 now lag 70 and 95
            ccs.step("INSERT INTO T VALUES(95, 7);", """
                     ts  | x | p | weight
                    ----------------------
                     95  | 7 | 2 | 1
                     100 | 0 | 2 | -1
                     100 | 0 | 3 | 1
                     101 | 0 | 3 | -1
                     101 | 0 | 7 | 1""");
            ccs.step("REMOVE FROM T VALUES(95, 7);", """
                     ts  | x | p | weight
                    ----------------------
                     95  | 7 | 2 | -1
                     100 | 0 | 3 | -1
                     100 | 0 | 2 | 1
                     101 | 0 | 7 | -1
                     101 | 0 | 3 | 1""");
        }
    }

    /** A row of the tables: partition p, ordering column ts, value x. */
    record Row(int p, @Nullable Integer ts, long x) {}

    static Row row(int p, @Nullable Integer ts, long x) {
        return new Row(p, ts, x);
    }

    /** Two pairs with LATENESS on ts: N, where ts is NOT NULL, and L, where ts is nullable.
     * <pre>
     * CREATE TABLE LATE_N (p INT NOT NULL, ts INT NOT NULL LATENESS 10, x BIGINT NOT NULL);
     * CREATE TABLE PLAIN_N (p INT NOT NULL, ts INT NOT NULL, x BIGINT NOT NULL);
     * </pre> */
    static List<TablePair<Row>> createTablePairs() {
        List<TablePair<Row>> result = new ArrayList<>();
        for (boolean nullable : new boolean[] { false, true })
            result.add(new TablePair<>(nullable ? "L" : "N", nullable, Row.class, Set.of("ts"), 10));
        return result;
    }

    /** Orderings: name, table pair (N has a NOT NULL ts, L a nullable ts), ORDER BY clause.  The
     * state of the operator is garbage collected in the first three. */
    static final String[][] ORDERINGS = {
            { "asc", "N", "ts" },
            { "ascDesc", "N", "ts, x DESC" },
            { "nullsLast", "L", "ts NULLS LAST" },
            { "nullsFirst", "L", "ts NULLS FIRST" },
            { "desc", "N", "ts DESC" },
    };

    static final String[] FUNCTIONS = { "LAG(x)", "LAG(x, 2)", "LEAD(x)", "LEAD(x, 3)" };

    /** The columns of LATE_ALL and PLAIN_ALL: the ordering, the row, and one column per function. */
    static List<String> columns() {
        List<String> result = new ArrayList<>(List.of("ordering", "p", "ts", "x"));
        for (int i = 0; i < FUNCTIONS.length; i++)
            result.add("f" + i);
        return result;
    }

    /* The generated program starts with
     * CREATE TABLE LATE_N (p INT NOT NULL, ts INT NOT NULL LATENESS 10, x BIGINT NOT NULL);
     * ...
     * CREATE LOCAL VIEW LATE_ALL AS SELECT 'asc' AS ordering, CAST(p AS VARCHAR) AS p,
     *   CAST(ts AS VARCHAR) AS ts, CAST(x AS VARCHAR) AS x,
     *   CAST(LAG(x) OVER (PARTITION BY p ORDER BY ts) AS VARCHAR) AS f0,
     *   ... FROM LATE_N
     * UNION ALL ...;
     */
    static String differentialProgram(List<TablePair<Row>> pairs) {
        StringBuilder sql = new StringBuilder();
        for (TablePair<Row> pair : pairs) {
            sql.append(pair.create("LATE_", true));
            sql.append(pair.create("PLAIN_", false));
        }
        for (String prefix : TablePair.PREFIXES) {
            List<String> branches = new ArrayList<>();
            for (String[] ordering : ORDERINGS) {
                List<String> functions = new ArrayList<>();
                for (int i = 0; i < FUNCTIONS.length; i++)
                    functions.add("CAST(" + FUNCTIONS[i] + " OVER (PARTITION BY p ORDER BY " + ordering[2] +
                            ") AS VARCHAR) AS f" + i);
                // The casts remove the waterlines, so that GC does not prune the traces of the operators
                // that compute view D, which compares the two sides
                branches.add("SELECT '" + ordering[0] + "' AS ordering, CAST(p AS VARCHAR) AS p, " +
                        "CAST(ts AS VARCHAR) AS ts, CAST(x AS VARCHAR) AS x, " + String.join(", ", functions) +
                        " FROM " + prefix + ordering[1]);
            }
            sql.append("CREATE LOCAL VIEW ").append(prefix).append("ALL AS ")
                    .append(String.join("\nUNION ALL ", branches)).append(";\n");
        }
        sql.append(DifferentialTester.differenceView(columns()));
        return sql.toString();
    }

    /** Every function, ordering, and change around the waterline gives the same output with and
     * without LATENESS. */
    @Test
    public void sameOutputWithoutLateness() {
        List<TablePair<Row>> pairs = createTablePairs();
        var tester = new DifferentialTester(this.getCCS(differentialProgram(pairs)), pairs, columns());
        // A dense grid in partition 0, a sparse one in partition 1, and a NULL ts in each
        List<Row> grid = new ArrayList<>();
        for (int ts = 100; ts < 200; ts++) {
            grid.add(row(0, ts, ts));
            if (ts % 7 == 2)
                grid.add(row(1, ts, 1000 + ts));
        }
        // Copies of the last rows below the waterline of the next steps, which lead to different rows
        grid.add(row(0, 191, 191));
        grid.add(row(0, 191, 191));
        grid.add(row(1, 191, 1191));
        grid.add(row(1, 191, 1191));
        grid.add(row(0, null, 5));
        grid.add(row(1, null, 6));
        tester.insert(grid.toArray(new Row[0]));
        // Move the waterline to 192, so that compaction applies GC
        tester.insert(row(0, 200, 200), row(0, 201, 201), row(0, 202, 202));
        // Changes at and just above the waterline
        tester.insert(Set.of("N", "L"), row(0, 192, 2), row(1, 192, 3));
        tester.delete(Set.of("N", "L"), row(0, 195, 195), row(1, 198, 1198));
        tester.insert(Set.of("N", "L"), row(0, 194, 4), row(0, 194, 4));
        tester.delete(Set.of("N", "L"), row(0, 192, 2));
        // Rows with a NULL ts, which are never late
        tester.insert(Set.of("L"), row(0, null, 7), row(1, null, 8));
        tester.delete(Set.of("L"), row(0, null, 5));
        // Move the waterline by one in each iteration.  The step that inserts row ts of
        // partition 0 also inserts a row of partition 1 at the waterline, ts - 11; the step
        // after it deletes the row of partition 0 at the new waterline, ts - 10
        for (int ts = 210; ts < 230; ts++) {
            tester.insert(Set.of("N", "L"), row(0, ts, ts), row(1, ts - 11, 2000 + ts));
            if (ts >= 220)
                tester.delete(Set.of("N", "L"), row(0, ts - 10, ts - 10));
        }
    }

    /** Counts the LAG operators of a circuit that do and do not garbage collect their state. */
    static class CountLagOperators extends CircuitVisitor {
        int plain = 0;
        int withWaterline = 0;

        CountLagOperators(CompilerCircuit circuit) {
            super(circuit.compiler);
        }

        @Override
        public void postorder(DBSPLagOperator operator) {
            this.plain++;
        }

        @Override
        public void postorder(DBSPLagWithWaterlineOperator operator) {
            this.withWaterline++;
        }
    }

    /** The orderings in which the waterline garbage collects the state of LAG and LEAD: the
     * first ORDER BY column has the waterline, is ascending, and sorts its NULL values last. */
    @Test
    public void garbageCollectedOrderings() {
        // PARTITION BY and ORDER BY clauses, the table, and whether the operators garbage collect
        String[][] cases = {
                { "PARTITION BY p ORDER BY ts", "N", "true" },
                { "PARTITION BY p ORDER BY ts, x DESC", "N", "true" },
                { "ORDER BY ts", "N", "true" },
                // An ascending order sorts NULL values first unless it says otherwise
                { "PARTITION BY p ORDER BY ts", "L", "false" },
                { "PARTITION BY p ORDER BY ts NULLS LAST", "L", "true" },
                { "PARTITION BY p ORDER BY ts NULLS FIRST", "L", "false" },
                // A NOT NULL column has no NULL values to sort first
                { "PARTITION BY p ORDER BY ts NULLS FIRST", "N", "true" },
                { "PARTITION BY p ORDER BY ts DESC", "N", "false" },
                { "PARTITION BY p ORDER BY x, ts", "N", "false" },
                { "PARTITION BY ts ORDER BY x", "N", "false" },
        };
        for (String[] c : cases) {
            String sql = "CREATE TABLE T (p INT NOT NULL, ts INT " + (c[1].equals("N") ? "NOT NULL " : "") +
                    "LATENESS 10, x BIGINT NOT NULL);\n" +
                    "CREATE VIEW V AS SELECT p, ts, x, LAG(x) OVER (" + c[0] + ") AS l, " +
                    "LEAD(x, 2) OVER (" + c[0] + ") AS n FROM T;";
            var cc = this.getCC(sql);
            CountLagOperators count = new CountLagOperators(cc);
            cc.visit(count);
            boolean gc = Boolean.parseBoolean(c[2]);
            Assert.assertEquals(c[0] + " over " + c[1], gc ? 2 : 0, count.withWaterline);
            Assert.assertEquals(c[0] + " over " + c[1], gc ? 0 : 2, count.plain);
        }
    }
}
