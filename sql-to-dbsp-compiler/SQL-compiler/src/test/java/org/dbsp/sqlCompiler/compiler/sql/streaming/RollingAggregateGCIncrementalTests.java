package org.dbsp.sqlCompiler.compiler.sql.streaming;

import org.dbsp.sqlCompiler.circuit.operator.DBSPAggregateOperatorBase;
import org.dbsp.sqlCompiler.compiler.sql.StreamingTestBase;
import org.dbsp.sqlCompiler.compiler.sql.tools.CompilerCircuitStream;
import org.dbsp.sqlCompiler.compiler.sql.tools.DifferentialTester;
import org.dbsp.sqlCompiler.compiler.sql.tools.TablePair;
import org.dbsp.sqlCompiler.compiler.visitors.inner.InnerVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.compiler.visitors.outer.FuseIntoArrayAgg;
import org.dbsp.sqlCompiler.ir.type.user.DBSPTypeArray;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** GC of rolling aggregates, and the state of ARRAY_AGG after {@link FuseIntoArrayAgg}. */
public class RollingAggregateGCIncrementalTests extends StreamingTestBase {
    /** Insert a row whose window covers row 60 once the waterline of ts is far past 60.
     * Expected outputs validated with Postgres. */
    void checkForwardWindow(String window) {
        String sql = """
                CREATE TABLE T (ts BIGINT NOT NULL LATENESS 10, x BIGINT NOT NULL);
                CREATE VIEW V AS SELECT ts, x, SUM(x) OVER (WINDOW) AS s FROM T;"""
                .replace("WINDOW", window);
        String[] programs = {
                // As written
                sql,
                // Without LATENESS
                sql.replace(" LATENESS 10", "")
        };
        for (String program : programs) {
            var ccs = this.getCCS(program).compactAfterEachStep();
            ccs.step("INSERT INTO T VALUES(60, 1);", """
                     ts | x | s | weight
                    ---------------------
                     60 | 1 | 1 | 1""");
            // Move the waterline of ts to 98, one row per step, so that compaction applies GC
            for (int ts = 100; ts < 109; ts++)
                ccs.step("INSERT INTO T VALUES(" + ts + ", 0);", """
                         ts | x | s | weight
                        ---------------------
                         TS | 0 | 0 | 1""".replace("TS", Integer.toString(ts)));
            // Not late; the window of every earlier row contains 109
            ccs.step("INSERT INTO T VALUES(109, 7);", """
                     ts  | x | s | weight
                    ----------------------
                     60  | 1 | 1 | -1
                     60  | 1 | 8 | 1
                     100 | 0 | 0 | -1
                     100 | 0 | 7 | 1
                     101 | 0 | 0 | -1
                     101 | 0 | 7 | 1
                     102 | 0 | 0 | -1
                     102 | 0 | 7 | 1
                     103 | 0 | 0 | -1
                     103 | 0 | 7 | 1
                     104 | 0 | 0 | -1
                     104 | 0 | 7 | 1
                     105 | 0 | 0 | -1
                     105 | 0 | 7 | 1
                     106 | 0 | 0 | -1
                     106 | 0 | 7 | 1
                     107 | 0 | 0 | -1
                     107 | 0 | 7 | 1
                     108 | 0 | 0 | -1
                     108 | 0 | 7 | 1
                     109 | 7 | 7 | 1""");
        }
    }

    @Test
    public void following() {
        this.checkForwardWindow("ORDER BY ts RANGE BETWEEN CURRENT ROW AND 50 FOLLOWING");
    }

    /** In descending order, 50 PRECEDING covers the next 50 larger values. */
    @Test
    public void descendingPreceding() {
        this.checkForwardWindow("ORDER BY ts DESC RANGE BETWEEN 50 PRECEDING AND CURRENT ROW");
    }

    @Test
    public void unboundedFollowing() {
        this.checkForwardWindow("ORDER BY ts RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING");
    }

    /** Every frame contains the NULL rows, so a NULL row inserted after the waterline passed
     * changes every output.  Expected outputs validated with Postgres. */
    @Test
    public void nullsFirstUnboundedPreceding() {
        String sql = """
                CREATE TABLE T (ts BIGINT LATENESS 10, x BIGINT NOT NULL);
                CREATE VIEW V AS SELECT ts, x, SUM(x) OVER (ORDER BY ts NULLS FIRST
                   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s FROM T;""";
        String[] programs = {
                // As written
                sql,
                // Without LATENESS
                sql.replace(" LATENESS 10", "")
        };
        for (String program : programs) {
            var ccs = this.getCCS(program).compactAfterEachStep();
            ccs.step("INSERT INTO T VALUES(60, 1);", """
                     ts | x | s | weight
                    ---------------------
                     60 | 1 | 1 | 1""");
            // Move the waterline of ts to 98, one row per step, so that compaction applies GC
            for (int ts = 100; ts < 109; ts++)
                ccs.step("INSERT INTO T VALUES(" + ts + ", 0);", """
                         ts | x | s | weight
                        ---------------------
                         TS | 0 | 1 | 1""".replace("TS", Integer.toString(ts)));
            ccs.step("INSERT INTO T VALUES(NULL, 5);", """
                     ts  | x | s | weight
                    ----------------------
                         | 5 | 5 | 1
                     60  | 1 | 1 | -1
                     60  | 1 | 6 | 1
                     100 | 0 | 1 | -1
                     100 | 0 | 6 | 1
                     101 | 0 | 1 | -1
                     101 | 0 | 6 | 1
                     102 | 0 | 1 | -1
                     102 | 0 | 6 | 1
                     103 | 0 | 1 | -1
                     103 | 0 | 6 | 1
                     104 | 0 | 1 | -1
                     104 | 0 | 6 | 1
                     105 | 0 | 1 | -1
                     105 | 0 | 6 | 1
                     106 | 0 | 1 | -1
                     106 | 0 | 6 | 1
                     107 | 0 | 1 | -1
                     107 | 0 | 6 | 1
                     108 | 0 | 1 | -1
                     108 | 0 | 6 | 1""");
        }
    }

    /** A row at the waterline needs the inputs up to 50 below it, although the window
     * is only 40 wide.  Expected outputs validated with Postgres. */
    @Test
    public void precedingUpperBound() {
        String sql = """
                CREATE TABLE T (ts BIGINT NOT NULL LATENESS 10, x BIGINT NOT NULL);
                CREATE VIEW V AS SELECT ts, x, SUM(x) OVER (ORDER BY ts
                   RANGE BETWEEN 50 PRECEDING AND 10 PRECEDING) AS s FROM T;""";
        String[] programs = {
                // As written
                sql,
                // Without LATENESS
                sql.replace(" LATENESS 10", "")
        };
        for (String program : programs) {
            var ccs = this.getCCS(program).compactAfterEachStep();
            ccs.step("INSERT INTO T VALUES(50, 1);", """
                     ts | x | s   | weight
                    -----------------------
                     50 | 1 |NULL| 1""");
            ccs.step("INSERT INTO T VALUES(100, 0);", """
                     ts  | x | s | weight
                    ----------------------
                     100 | 0 | 1 | 1""");
            // Move the waterline of ts to 95, one row per step, so that compaction applies GC
            for (int ts = 101; ts < 106; ts++)
                ccs.step("INSERT INTO T VALUES(" + ts + ", 0);", """
                         ts | x | s   | weight
                        -----------------------
                         TS | 0 |NULL| 1""".replace("TS", Integer.toString(ts)));
            // Not late; the window of 95 is [45, 85], which contains 50
            ccs.step("INSERT INTO T VALUES(95, 0);", """
                     ts  | x | s   | weight
                    ------------------------
                     95  | 0 | 1   | 1
                     105 | 0 |NULL| -1
                     105 | 0 | 0   | 1""");
        }
    }

    /** Window frames, one for each valid pair of bound kinds. */
    static final String[] FRAMES = {
            "UNBOUNDED PRECEDING AND 10 PRECEDING",
            "UNBOUNDED PRECEDING AND CURRENT ROW",
            "UNBOUNDED PRECEDING AND 30 FOLLOWING",
            "UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
            "30 PRECEDING AND 10 PRECEDING",
            "30 PRECEDING AND CURRENT ROW",
            "30 PRECEDING AND 30 FOLLOWING",
            "30 PRECEDING AND UNBOUNDED FOLLOWING",
            "CURRENT ROW AND CURRENT ROW",
            "CURRENT ROW AND 30 FOLLOWING",
            "CURRENT ROW AND UNBOUNDED FOLLOWING",
            "10 FOLLOWING AND 30 FOLLOWING",
            "10 FOLLOWING AND UNBOUNDED FOLLOWING",
    };

    /** A row of the tables: partition p, ordering column ts, summed value x. */
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

    /** Orderings: name, table pair (N has a NOT NULL ts, L a nullable ts), ORDER BY clause. */
    static final String[][] ORDERINGS = {
            { "asc", "N", "ts" },
            { "nullsFirst", "L", "ts NULLS FIRST" },
            { "nullsLast", "L", "ts NULLS LAST" },
            { "desc", "N", "ts DESC" },
    };

    /** The columns of LATE_ALL and PLAIN_ALL: the ordering, the row, and one sum per frame. */
    static List<String> columns() {
        List<String> result = new ArrayList<>(List.of("ordering", "p", "ts", "x"));
        for (int i = 0; i < FRAMES.length; i++)
            result.add("s" + i);
        return result;
    }

    /* The generated program starts with
     * CREATE TABLE LATE_N (p INT NOT NULL, ts INT NOT NULL LATENESS 10, x BIGINT NOT NULL);
     * ...
     * CREATE LOCAL VIEW LATE_ALL AS SELECT 'asc' AS ordering, CAST(p AS VARCHAR) AS p,
     *   CAST(ts AS VARCHAR) AS ts, CAST(x AS VARCHAR) AS x,
     *   CAST(SUM(x) OVER (PARTITION BY p ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND 10 PRECEDING) AS VARCHAR) AS s0,
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
                boolean nullable = ordering[1].equals("L");
                List<String> sums = new ArrayList<>();
                for (int i = 0; i < FRAMES.length; i++) {
                    // A frame with an offset requires an ORDER BY column that cannot be NULL
                    if (nullable && FRAMES[i].matches(".*[0-9].*"))
                        sums.add("CAST(NULL AS VARCHAR) AS s" + i);
                    else
                        sums.add("CAST(SUM(x) OVER (PARTITION BY p ORDER BY " + ordering[2] +
                                " RANGE BETWEEN " + FRAMES[i] + ") AS VARCHAR) AS s" + i);
                }
                // The casts remove the waterlines, so that GC does not prune the traces of the operators
                // that compute view D, which compares the two sides
                branches.add("SELECT '" + ordering[0] + "' AS ordering, CAST(p AS VARCHAR) AS p, " +
                        "CAST(ts AS VARCHAR) AS ts, CAST(x AS VARCHAR) AS x, " + String.join(", ", sums) +
                        " FROM " + prefix + ordering[1]);
            }
            sql.append("CREATE LOCAL VIEW ").append(prefix).append("ALL AS ")
                    .append(String.join("\nUNION ALL ", branches)).append(";\n");
        }
        sql.append(DifferentialTester.differenceView(columns()));
        return sql.toString();
    }

    /** Every frame, ordering, and change around the waterline gives the same output with and
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
        grid.add(row(0, null, 5));
        grid.add(row(1, null, 6));
        tester.insert(grid.toArray(new Row[0]));
        // Move the waterline to 192, so that compaction applies GC
        tester.insert(row(0, 200, 200), row(0, 201, 201), row(0, 202, 202));
        // Changes at and just above the waterline
        tester.insert(Set.of("N", "L"), row(0, 192, 2), row(1, 192, 3));
        tester.delete(Set.of("N", "L"), row(0, 195, 195), row(1, 198, 1198));
        tester.insert(Set.of("N", "L"), row(0, 194, 4));
        // Rows with a NULL ts, which are never late
        tester.insert(Set.of("L"), row(0, null, 7), row(1, null, 8));
        tester.delete(Set.of("L"), row(0, null, 5));
        // Move the waterline to 222 and change rows around it again
        tester.insert(row(0, 230, 230), row(0, 231, 231), row(0, 232, 232));
        tester.insert(Set.of("N", "L"), row(0, 222, 9), row(1, 222, 10));
        tester.delete(Set.of("N", "L"), row(0, 230, 230));
    }

    /** The number of array types in the values of all aggregate operators of the circuit */
    private static int storedArrays(CompilerCircuitStream ccs) {
        int[] count = new int[1];
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPAggregateOperatorBase operator) {
                new InnerVisitor(this.compiler()) {
                    @Override
                    public void postorder(DBSPTypeArray type) {
                        count[0]++;
                    }
                }.apply(operator.getOutputIndexedZSetType().elementType);
            }
        });
        return count[0];
    }

    private static int aggregates(CompilerCircuitStream ccs) {
        int[] count = new int[1];
        ccs.visit(new CircuitVisitor(ccs.compiler) {
            @Override
            public void postorder(DBSPAggregateOperatorBase operator) {
                count[0]++;
            }
        });
        return count[0];
    }

    @Test
    public void issue7507() {
        // Validated with Postgres: ARRAY_AGG(c IGNORE NULLS) as array_agg(c) FILTER (WHERE c IS NOT NULL),
        // ARRAY_DISTINCT as ARRAY(SELECT DISTINCT unnest(..)), ARRAY_MAX(TRANSFORM(.., x -> x * 2)) as max(x * 2) over unnest
        // Fused: 2 aggregates of 3; SUM is not an ARRAY_AGG
        var ccs = this.getCCS("""
                CREATE TABLE t(k INT, c INT);
                CREATE VIEW v AS
                SELECT k, SUM(c) AS s,
                       CARDINALITY(ARRAY_DISTINCT(ARRAY_AGG(c IGNORE NULLS))) AS d,
                       ARRAY_MAX(TRANSFORM(ARRAY_AGG(c), x -> x * 2)) AS m
                FROM t GROUP BY k;""");
        ccs.step("INSERT INTO t VALUES (1, 10), (1, 10), (1, 20), (1, NULL), (2, NULL);",
                """
                         k | s  | d | m  | weight
                        -------------------------
                         1 | 40 | 2 | 40 | 1
                         2 |    | 0 |    | 1""");
        ccs.step("REMOVE FROM t VALUES (1, 20);",
                """
                         k | s  | d | m  | weight
                        -------------------------
                         1 | 40 | 2 | 40 | -1
                         1 | 20 | 1 | 20 | 1""");
        ccs.step("REMOVE FROM t VALUES (2, NULL);",
                """
                         k | s  | d | m  | weight
                        -------------------------
                         2 |    | 0 |    | -1""");
        Assert.assertEquals(0, storedArrays(ccs));
    }

    @Test
    public void issue7507OneOfTwo() {
        // Validated with Postgres: ARRAY_MAX and ARRAY_MIN as max and min over unnest
        // Fused: 1 aggregate of 2; the second is read by two columns
        var ccs = this.getCCS("""
                CREATE TABLE t(k INT, c INT);
                CREATE VIEW v AS
                SELECT k, CARDINALITY(ARRAY_DISTINCT(ARRAY_AGG(c IGNORE NULLS))) AS d,
                       ARRAY_MAX(ARRAY_AGG(c)) AS m, ARRAY_MIN(ARRAY_AGG(c)) AS n
                FROM t GROUP BY k;""");
        ccs.step("INSERT INTO t VALUES (1, 10), (1, 10), (1, 20), (1, NULL), (2, NULL);",
                """
                         k | d | m  | n  | weight
                        -------------------------
                         1 | 2 | 20 | 10 | 1
                         2 | 0 |    |    | 1""");
        ccs.step("REMOVE FROM t VALUES (1, 20);",
                """
                         k | d | m  | n  | weight
                        -------------------------
                         1 | 2 | 20 | 10 | -1
                         1 | 1 | 10 | 10 | 1""");
        // Both ARRAY_AGGs are computed by one fold, which keeps one array
        Assert.assertEquals(1, storedArrays(ccs));
    }

    @Test
    public void issue7507Window() {
        // Validated with Postgres: array_agg(c) FILTER (WHERE c IS NOT NULL) OVER (..)
        // Fused: 1 aggregate of 1
        var ccs = this.getCCS("""
                CREATE TABLE t(p INT, ts INT NOT NULL, c INT);
                CREATE VIEW v AS
                SELECT p, ts, c, CARDINALITY(ARRAY_DISTINCT(ARRAY_AGG(c IGNORE NULLS) OVER
                    (PARTITION BY p ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW))) AS d
                FROM t;""");
        ccs.step("INSERT INTO t VALUES (1, 1, 10), (1, 2, 10), (1, 3, 20), (1, 6, NULL), (2, 1, 5);",
                """
                         p | ts | c  | d | weight
                        -------------------------
                         1 | 1  | 10 | 1 | 1
                         1 | 2  | 10 | 1 | 1
                         1 | 3  | 20 | 2 | 1
                         1 | 6  |    | 0 | 1
                         2 | 1  | 5  | 1 | 1""");
        ccs.step("REMOVE FROM t VALUES (1, 2, 10);",
                """
                         p | ts | c  | d | weight
                        -------------------------
                         1 | 2  | 10 | 1 | -1""");
        Assert.assertEquals(0, storedArrays(ccs));
    }

    @Test
    public void issue7507Join() {
        // Validated with Postgres.  Fused: 1 aggregate of 1
        var ccs = this.getCCS("""
                CREATE TABLE t(k INT NOT NULL, c INT);
                CREATE VIEW v AS
                SELECT t.k, t.c, CARDINALITY(ARRAY_DISTINCT(a.arr)) AS d
                FROM t JOIN (SELECT k, ARRAY_AGG(c IGNORE NULLS) AS arr FROM t GROUP BY k) a ON t.k = a.k;""");
        ccs.step("INSERT INTO t VALUES (1, 10), (1, 30), (1, 20), (1, NULL), (2, NULL);",
                """
                         k | c  | d | weight
                        --------------------
                         1 | 10 | 3 | 1
                         1 | 20 | 3 | 1
                         1 | 30 | 3 | 1
                         1 |    | 3 | 1
                         2 |    | 0 | 1""");
        ccs.step("REMOVE FROM t VALUES (1, 20);",
                """
                         k | c  | d | weight
                        --------------------
                         1 | 10 | 3 | -1
                         1 | 20 | 3 | -1
                         1 | 30 | 3 | -1
                         1 |    | 3 | -1
                         1 | 10 | 2 | 1
                         1 | 30 | 2 | 1
                         1 |    | 2 | 1""");
        Assert.assertEquals(0, storedArrays(ccs));
    }

    @Test
    public void issue7507MapIndex() {
        // Validated with Postgres.  Fused: 1 aggregate of 1
        var ccs = this.getCCS("""
                CREATE TABLE t(k INT NOT NULL, c INT);
                CREATE TABLE s(x INT NOT NULL);
                CREATE VIEW w AS
                SELECT s.x, a.k
                FROM (SELECT k, ARRAY_AGG(c IGNORE NULLS) AS arr FROM t GROUP BY k) a
                JOIN s ON s.x = COALESCE(CARDINALITY(ARRAY_DISTINCT(a.arr)), 0);""");
        ccs.step("""
                INSERT INTO t VALUES (1, 10), (1, 30), (1, 20), (1, NULL), (2, NULL);
                INSERT INTO s VALUES (0), (1), (2), (3);""",
                """
                         x | k | weight
                        ---------------
                         0 | 2 | 1
                         3 | 1 | 1""");
        ccs.step("REMOVE FROM t VALUES (1, 20);",
                """
                         x | k | weight
                        ---------------
                         3 | 1 | -1
                         2 | 1 | 1""");
        Assert.assertEquals(0, storedArrays(ccs));
    }

    @Test
    public void issue7507TwoProducers() {
        // Validated with Postgres.  Fused: 2 aggregates of 2, one on each side of the join
        var ccs = this.getCCS("""
                CREATE TABLE t(k INT NOT NULL, c INT);
                CREATE TABLE s(k INT NOT NULL, c INT);
                CREATE VIEW v AS
                SELECT a.k, CARDINALITY(ARRAY_DISTINCT(a.arr)) AS d, ARRAY_MAX(TRANSFORM(b.arr, x -> x * 2)) AS m
                FROM (SELECT k, ARRAY_AGG(c IGNORE NULLS) AS arr FROM t GROUP BY k) a
                JOIN (SELECT k, ARRAY_AGG(c) AS arr FROM s GROUP BY k) b ON a.k = b.k;""");
        ccs.step("""
                INSERT INTO t VALUES (1, 10), (1, 10), (1, 20), (1, NULL), (2, NULL);
                INSERT INTO s VALUES (1, 5), (1, 7), (2, NULL), (3, 1);""",
                """
                         k | d | m  | weight
                        --------------------
                         1 | 2 | 14 | 1
                         2 | 0 |    | 1""");
        ccs.step("REMOVE FROM t VALUES (1, 20);",
                """
                         k | d | m  | weight
                        --------------------
                         1 | 2 | 14 | -1
                         1 | 1 | 14 | 1""");
        Assert.assertEquals(0, storedArrays(ccs));
    }

    @Test
    public void issue7507Windows() {
        // Validated with Postgres.  Fused: 3 aggregates of 3, behind one star join
        var ccs = this.getCCS("""
                CREATE TABLE t(p INT, ts INT NOT NULL, c INT);
                CREATE VIEW v AS
                SELECT p, ts, c,
                    CARDINALITY(ARRAY_DISTINCT(ARRAY_AGG(c IGNORE NULLS) OVER
                        (PARTITION BY p ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW))) AS d2,
                    CARDINALITY(ARRAY_DISTINCT(ARRAY_AGG(c IGNORE NULLS) OVER
                        (PARTITION BY p ORDER BY ts RANGE BETWEEN 3 PRECEDING AND CURRENT ROW))) AS d3,
                    CARDINALITY(ARRAY_DISTINCT(ARRAY_AGG(c IGNORE NULLS) OVER
                        (PARTITION BY p ORDER BY ts RANGE BETWEEN 4 PRECEDING AND CURRENT ROW))) AS d4
                FROM t;""");
        ccs.step("INSERT INTO t VALUES (1, 1, 10), (1, 2, 10), (1, 3, 20), (1, 6, NULL), (2, 1, 5);",
                """
                         p | ts | c  | d2 | d3 | d4 | weight
                        -----------------------------------
                         1 | 1  | 10 | 1  | 1  | 1  | 1
                         1 | 2  | 10 | 1  | 1  | 1  | 1
                         1 | 3  | 20 | 2  | 2  | 2  | 1
                         1 | 6  |    | 0  | 1  | 2  | 1
                         2 | 1  | 5  | 1  | 1  | 1  | 1""");
        ccs.step("REMOVE FROM t VALUES (1, 2, 10);",
                """
                         p | ts | c  | d2 | d3 | d4 | weight
                        -----------------------------------
                         1 | 2  | 10 | 1  | 1  | 1  | -1
                         1 | 6  |    | 0  | 1  | 2  | -1
                         1 | 6  |    | 0  | 1  | 1  | 1""");
        Assert.assertEquals(0, storedArrays(ccs));
    }

    @Test
    public void issue7507EmptyWindow() {
        // Fused: 1 aggregate of 1.  The frame excludes the current row, so the first row of each partition
        // has an empty frame, where ARRAY_AGG is an empty array and d is 0.  Validated with Postgres for
        // the other rows (Postgres returns NULL for empty data, we return an empty array.)
        var ccs = this.getCCS("""
                CREATE TABLE t(p INT, ts INT NOT NULL, c INT);
                CREATE VIEW v AS
                SELECT p, ts, c, CARDINALITY(ARRAY_DISTINCT(ARRAY_AGG(c IGNORE NULLS) OVER
                    (PARTITION BY p ORDER BY ts RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING))) AS d
                FROM t;""");
        ccs.step("INSERT INTO t VALUES (1, 1, 10), (1, 2, 10), (1, 3, 20), (1, 6, NULL), (2, 1, 5);",
                """
                         p | ts | c  | d | weight
                        -------------------------
                         1 | 1  | 10 | 0 | 1
                         1 | 2  | 10 | 1 | 1
                         1 | 3  | 20 | 1 | 1
                         1 | 6  |    | 1 | 1
                         2 | 1  | 5  | 0 | 1""");
        ccs.step("REMOVE FROM t VALUES (1, 2, 10);",
                """
                         p | ts | c  | d | weight
                        -------------------------
                         1 | 2  | 10 | 1 | -1""");
        Assert.assertEquals(0, storedArrays(ccs));
    }

    @Test
    public void issue7507Ordered() {
        // Validated with Postgres.  Fused: 2 aggregates of 2; the index is cheap, and its result is an INT
        var ccs = this.getCCS("""
                CREATE TABLE t(k INT, c INT);
                CREATE VIEW v AS
                SELECT k, (ARRAY_AGG(c ORDER BY c))[2] AS second, CARDINALITY(ARRAY_AGG(DISTINCT c)) AS d
                FROM t GROUP BY k;""");
        ccs.step("INSERT INTO t VALUES (1, 30), (1, 10), (1, 20), (1, 10), (2, 5);",
                """
                         k | second | d | weight
                        ------------------------
                         1 | 10     | 3 | 1
                         2 |        | 1 | 1""");
        ccs.step("REMOVE FROM t VALUES (1, 10);",
                """
                         k | second | d | weight
                        ------------------------
                         1 | 10     | 3 | -1
                         1 | 20     | 3 | 1""");
        ccs.step("REMOVE FROM t VALUES (1, 10);",
                """
                         k | second | d | weight
                        ------------------------
                         1 | 20     | 3 | -1
                         1 | 30     | 2 | 1""");
        Assert.assertEquals(0, storedArrays(ccs));
    }

    @Test
    public void issue7507ReadsKey() {
        // Fused: 0 aggregates of 1; the column reads the key and the array
        var ccs = this.getCCS("""
                CREATE TABLE t(k INT, c INT);
                CREATE VIEW v AS SELECT k, ARRAY_CONTAINS(ARRAY_AGG(c), k) AS h FROM t GROUP BY k;""");
        Assert.assertEquals(1, storedArrays(ccs));
    }

    @Test
    public void issue7507EmptyWindowNotNull() {
        // Fused: 1 aggregate of 1.  An empty frame gives an empty array also when c is NOT NULL, so n is 0
        // there.  Validated with Postgres for the other rows (Postgres differs for empty data)
        var ccs = this.getCCS("""
                CREATE TABLE t(p INT, ts INT NOT NULL, c INT NOT NULL);
                CREATE VIEW v AS
                SELECT p, ts, c, CARDINALITY(ARRAY_AGG(c) OVER
                    (PARTITION BY p ORDER BY ts RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING)) AS n
                FROM t;""");
        ccs.step("INSERT INTO t VALUES (1, 1, 10), (1, 2, 10), (1, 3, 20), (1, 6, 30), (2, 1, 5);",
                """
                         p | ts | c  | n | weight
                        -------------------------
                         1 | 1  | 10 | 0 | 1
                         1 | 2  | 10 | 1 | 1
                         1 | 3  | 20 | 2 | 1
                         1 | 6  | 30 | 1 | 1
                         2 | 1  | 5  | 0 | 1""");
        ccs.step("REMOVE FROM t VALUES (1, 3, 20);",
                """
                         p | ts | c  | n | weight
                        -------------------------
                         1 | 3  | 20 | 2 | -1
                         1 | 6  | 30 | 1 | -1
                         1 | 6  | 30 | 0 | 1""");
        Assert.assertEquals(0, storedArrays(ccs));
    }

    @Test
    public void issue7507DivideWindow() {
        // Validated with Postgres.  Fused: 1 aggregate of 1; no frame is empty, so g([]), a division
        // by 0, must not be computed
        var ccs = this.getCCS("""
                CREATE TABLE t(p INT, ts INT NOT NULL, c INT);
                CREATE VIEW v AS
                SELECT p, ts, 100 / CARDINALITY(ARRAY_AGG(c) OVER
                    (PARTITION BY p ORDER BY ts RANGE BETWEEN 2 PRECEDING AND CURRENT ROW)) AS q
                FROM t;""");
        ccs.step("INSERT INTO t VALUES (1, 1, 10), (1, 2, 20);",
                """
                         p | ts | q   | weight
                        ----------------------
                         1 | 1  | 100 | 1
                         1 | 2  | 50  | 1""");
        ccs.step("REMOVE FROM t VALUES (1, 1, 10);",
                """
                         p | ts | q   | weight
                        ----------------------
                         1 | 1  | 100 | -1
                         1 | 2  | 50  | -1
                         1 | 2  | 100 | 1""");
        Assert.assertEquals(0, storedArrays(ccs));
    }

    @Test
    public void issue7507DivideGroup() {
        // Validated with Postgres.
        var ccs = this.getCCS("""
                CREATE TABLE t(k INT, c INT);
                CREATE VIEW v AS SELECT k, 100 / CARDINALITY(ARRAY_AGG(c)) AS q FROM t GROUP BY k;""");
        ccs.step("INSERT INTO t VALUES (1, 10), (1, 20), (2, 5);",
                """
                         k | q   | weight
                        ---------------
                         1 | 50  | 1
                         2 | 100 | 1""");
        ccs.step("REMOVE FROM t VALUES (1, 20);",
                """
                         k | q   | weight
                        ---------------
                         1 | 50  | -1
                         1 | 100 | 1""");
        Assert.assertEquals(0, storedArrays(ccs));
    }

    @Test
    public void issue7507Shared() {
        var ccs = this.getCCS("""
                CREATE TABLE t(k INT, c INT);
                CREATE VIEW l AS SELECT k, ARRAY_AGG(c) AS arr FROM t GROUP BY k;
                CREATE VIEW n AS SELECT k, CARDINALITY(ARRAY_AGG(c)) AS n FROM t GROUP BY k;""");
        Assert.assertEquals(1, aggregates(ccs));
    }
}
